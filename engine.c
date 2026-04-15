/*
 * engine.c  –  Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MSG_LEN     4096
#define CMD_LEN             256
#define LOG_CHUNK           4096
#define LOG_BUF_CAP         32
#define DEFAULT_SOFT        (40UL << 20)
#define DEFAULT_HARD        (64UL << 20)
#define MAX_READERS         64
#define MONITOR_DEV         "/dev/container_monitor"
#define READY_TIMEOUT_SEC   5

static void scopy(char *dst, const char *src, size_t dst_size)
{
    if (!dst_size) return;
    size_t n = strlen(src);
    if (n >= dst_size) n = dst_size - 1;
    memcpy(dst, src, n);
    dst[n] = '\0';
}

typedef enum { CMD_SUPERVISOR=0, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP } cmd_kind_t;
typedef enum { CS_STARTING=0, CS_RUNNING, CS_STOPPED, CS_KILLED, CS_EXITED } cstate_t;

typedef struct container_rec {
    char   id[CONTAINER_ID_LEN];
    pid_t  host_pid;
    time_t started_at;
    cstate_t state;
    unsigned long soft_bytes, hard_bytes;
    int    exit_code, exit_signal;
    int    stop_requested;
    char   log_path[PATH_MAX];
    struct container_rec *next;
} container_rec_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK];
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUF_CAP];
    size_t          head, tail, count;
    int             shutting_down;
    pthread_mutex_t mu;
    pthread_cond_t  not_empty, not_full;
} bbuf_t;

typedef struct {
    cmd_kind_t    kind;
    char          container_id[CONTAINER_ID_LEN];
    char          rootfs[PATH_MAX];
    char          command[CMD_LEN];
    unsigned long soft_bytes, hard_bytes;
    int           nice_val;
} ctrl_req_t;

typedef struct {
    int  status;
    char message[CONTROL_MSG_LEN];
} ctrl_resp_t;

typedef struct {
    char  id[CONTAINER_ID_LEN];
    char  rootfs[PATH_MAX];
    char  command[CMD_LEN];
    int   nice_val;
    int   log_wfd;
    int   ready_wfd;
} child_cfg_t;

typedef struct {
    int    read_fd;
    char   container_id[CONTAINER_ID_LEN];
    bbuf_t *buf;
} reader_arg_t;

struct sv_ctx;
typedef struct { struct sv_ctx *ctx; int conn_fd; } conn_arg_t;

typedef struct {
    pthread_t       tids[MAX_READERS];
    int             count;
    pthread_mutex_t mu;
} rpool_t;

typedef struct sv_ctx {
    int             server_fd, monitor_fd;
    volatile int    should_stop;
    pthread_t       logger_tid;
    bbuf_t          logbuf;
    pthread_mutex_t meta_mu;
    container_rec_t *containers;
    rpool_t         rpool;
} sv_ctx_t;

static volatile sig_atomic_t g_sigchld        = 0;
static sv_ctx_t             *g_ctx            = NULL;
static volatile sig_atomic_t g_cli_interrupted= 0;
static char                  g_cli_run_id[CONTAINER_ID_LEN];

static int full_read(int fd, void *dst, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char*)dst + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int full_write(int fd, const void *src, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, (const char*)src + done, n - done);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

static void usage(const char *p) {
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n  %s logs <id>\n  %s stop <id>\n", p, p, p, p, p, p);
}

static const char *state_str(cstate_t s) {
    switch (s) {
    case CS_STARTING: return "starting"; 
    case CS_RUNNING:  return "running";
    case CS_STOPPED:  return "stopped";  
    case CS_KILLED:   return "killed";
    case CS_EXITED:   return "exited";   
    default:          return "unknown";
    }
}

static int parse_mib(const char *flag, const char *val, unsigned long *out) {
    char *end = NULL; 
    unsigned long v;
    (void)flag; /* Mark unused parameter to silence warning */
    
    errno = 0; 
    v = strtoul(val, &end, 10);
    if (errno || end == val || *end || v > ULONG_MAX / (1UL<<20)) {
        return -1;
    }
    *out = v << 20; 
    return 0;
}

static int parse_opts(ctrl_req_t *r, int argc, char *argv[], int si) {
    for (int i = si; i < argc; i += 2) {
        if (i+1 >= argc) return -1;
        
        if (!strcmp(argv[i],"--soft-mib")) { 
            if (parse_mib("--soft-mib",argv[i+1],&r->soft_bytes)) return -1; 
        }
        else if (!strcmp(argv[i],"--hard-mib")) { 
            if (parse_mib("--hard-mib",argv[i+1],&r->hard_bytes)) return -1; 
        }
        else if (!strcmp(argv[i],"--nice")) {
            char *end=NULL; 
            long v; 
            errno=0; 
            v=strtol(argv[i+1],&end,10);
            if (errno||end==argv[i+1]||*end||v<-20||v>19) return -1;
            r->nice_val=(int)v;
        } 
        else {
            return -1;
        }
    }
    if (r->soft_bytes > r->hard_bytes) {
        return -1;
    }
    return 0;
}

static int bbuf_init(bbuf_t *b) {
    memset(b,0,sizeof(*b));
    if (pthread_mutex_init(&b->mu,NULL)) return -1;
    if (pthread_cond_init(&b->not_empty,NULL)) return -1;
    if (pthread_cond_init(&b->not_full,NULL)) return -1;
    return 0;
}

static void bbuf_destroy(bbuf_t *b) {
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mu);
}

static void bbuf_shutdown(bbuf_t *b) {
    pthread_mutex_lock(&b->mu);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mu);
}

static int bbuf_push(bbuf_t *b, const log_item_t *item) {
    pthread_mutex_lock(&b->mu);
    while (b->count == LOG_BUF_CAP) {
        if (b->shutting_down) { 
            pthread_mutex_unlock(&b->mu); 
            return -1; 
        }
        pthread_cond_wait(&b->not_full, &b->mu);
    }
    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUF_CAP; 
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mu);
    return 0;
}

static int bbuf_pop(bbuf_t *b, log_item_t *item) {
    pthread_mutex_lock(&b->mu);
    while (b->count == 0 && !b->shutting_down) {
        pthread_cond_wait(&b->not_empty, &b->mu);
    }
    if (b->count == 0) { 
        pthread_mutex_unlock(&b->mu); 
        return 1; 
    }
    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUF_CAP; 
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mu);
    return 0;
}

#define LOG_FD_CACHE 64
typedef struct { char id[CONTAINER_ID_LEN]; int fd; } logfd_t;

static void *logging_thread(void *arg) {
    sv_ctx_t *ctx = (sv_ctx_t*)arg; 
    log_item_t item; 
    logfd_t cache[LOG_FD_CACHE]; 
    int nc = 0, i;
    
    memset(cache, 0, sizeof(cache));
    for (i = 0; i < LOG_FD_CACHE; i++) cache[i].fd = -1;
    
    while (!bbuf_pop(&ctx->logbuf, &item)) {
        int lfd = -1;
        for (i = 0; i < nc; i++) {
            if (!strncmp(cache[i].id, item.container_id, CONTAINER_ID_LEN)) { 
                lfd = cache[i].fd; 
                break; 
            }
        }
        if (lfd < 0) {
            char path[PATH_MAX]; 
            snprintf(path, sizeof(path), LOG_DIR "/%s.log", item.container_id);
            mkdir(LOG_DIR, 0755); 
            lfd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0644);
            if (lfd >= 0 && nc < LOG_FD_CACHE) { 
                scopy(cache[nc].id, item.container_id, CONTAINER_ID_LEN); 
                cache[nc].fd = lfd; 
                nc++; 
            }
        }
        if (lfd >= 0) {
            ssize_t w=0, tot=(ssize_t)item.length;
            while (w < tot) { 
                ssize_t n = write(lfd, item.data+w, (size_t)(tot-w)); 
                if (n <= 0) break; 
                w += n; 
            }
        }
    }
    for (i = 0; i < nc; i++) {
        if (cache[i].fd >= 0) {
            close(cache[i].fd);
        }
    }
    return NULL;
}

static void *reader_thread(void *arg) {
    reader_arg_t *a = (reader_arg_t*)arg; 
    char buf[LOG_CHUNK]; 
    ssize_t n;
    while ((n = read(a->read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item; 
        memset(&item, 0, sizeof(item));
        scopy(item.container_id, a->container_id, CONTAINER_ID_LEN);
        item.length = (size_t)n; 
        memcpy(item.data, buf, (size_t)n);
        if (bbuf_push(a->buf, &item) != 0) break;
    }
    close(a->read_fd); 
    free(a); 
    return NULL;
}

static void rpool_init(rpool_t *p) { 
    memset(p, 0, sizeof(*p)); 
    pthread_mutex_init(&p->mu, NULL); 
}

static void rpool_add(rpool_t *p, pthread_t tid) {
    pthread_mutex_lock(&p->mu);
    if (p->count < MAX_READERS) {
        p->tids[p->count++] = tid; 
    } else {
        pthread_detach(tid);
    }
    pthread_mutex_unlock(&p->mu);
}

static void rpool_join(rpool_t *p) {
    pthread_mutex_lock(&p->mu); 
    int n = p->count; 
    pthread_t tmp[MAX_READERS];
    memcpy(tmp, p->tids, (size_t)n * sizeof(pthread_t)); 
    pthread_mutex_unlock(&p->mu);
    for (int i = 0; i < n; i++) pthread_join(tmp[i], NULL);
}

static void rpool_destroy(rpool_t *p) { 
    pthread_mutex_destroy(&p->mu); 
}

static int reg_monitor(int mfd, const char *id, pid_t pid, unsigned long soft, unsigned long hard) {
    struct monitor_request r; 
    memset(&r, 0, sizeof(r));
    r.pid = pid; 
    r.soft_limit_bytes = soft; 
    r.hard_limit_bytes = hard;
    scopy(r.container_id, id, sizeof(r.container_id));
    return (ioctl(mfd, MONITOR_REGISTER, &r) < 0) ? -1 : 0;
}

static int unreg_monitor(int mfd, const char *id, pid_t pid) {
    struct monitor_request r; 
    memset(&r, 0, sizeof(r)); 
    r.pid = pid;
    scopy(r.container_id, id, sizeof(r.container_id));
    return (ioctl(mfd, MONITOR_UNREGISTER, &r) < 0) ? -1 : 0;
}

int child_fn(void *arg) {
    child_cfg_t *cfg = (child_cfg_t*)arg; 
    ssize_t wr;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) perror("sethostname");
    if (mount("none", "/", NULL, MS_REC|MS_PRIVATE, NULL) < 0) perror("mount private");

    char proc_path[PATH_MAX + 32];
    snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
    mkdir(proc_path, 0555);
    if (mount("proc", proc_path, "proc", 0, NULL) < 0) perror("mount proc");

    if (chroot(cfg->rootfs) < 0) { perror("chroot"); return 1; }
    if (chdir("/")          < 0) { perror("chdir");  return 1; }

    if (cfg->log_wfd >= 0) {
        if (dup2(cfg->log_wfd, STDOUT_FILENO) < 0) perror("dup2 stdout");
        if (dup2(cfg->log_wfd, STDERR_FILENO) < 0) perror("dup2 stderr");
        if (cfg->log_wfd != STDOUT_FILENO && cfg->log_wfd != STDERR_FILENO) {
            close(cfg->log_wfd);
        }
    }

    if (cfg->nice_val != 0) { 
        errno = 0; 
        if (nice(cfg->nice_val) == -1 && errno) perror("nice"); 
    }

    if (cfg->ready_wfd >= 0) {
        char rdy = 'R'; 
        wr = write(cfg->ready_wfd, &rdy, 1);
        if (wr < 0) perror("ready write");
        close(cfg->ready_wfd);
    }

    /* FIX 3: Force the shell to exec the command so it becomes PID 1 */
    char exec_cmd[CMD_LEN + 10];
    snprintf(exec_cmd, sizeof(exec_cmd), "exec %s", cfg->command);
    execl("/bin/sh", "/bin/sh", "-c", exec_cmd, (char*)NULL);
    
    perror("execl");
    return 127;
}

static container_rec_t *find_container(sv_ctx_t *ctx, const char *id) {
    for (container_rec_t *c = ctx->containers; c; c = c->next) {
        if (!strncmp(c->id, id, CONTAINER_ID_LEN)) return c;
    }
    return NULL;
}

static void prepend_container(sv_ctx_t *ctx, container_rec_t *c) { 
    c->next = ctx->containers; 
    ctx->containers = c; 
}

static void remove_container(sv_ctx_t *ctx, container_rec_t *target) {
    container_rec_t **pp = &ctx->containers;
    while (*pp) { 
        if (*pp == target) { 
            *pp = target->next; 
            return; 
        } 
        pp = &(*pp)->next; 
    }
}

static void h_sigchld(int s) { (void)s; g_sigchld = 1; }
static void h_sigterm(int s) { (void)s; if (g_ctx) g_ctx->should_stop = 1; }
static void h_sigcli (int s) { (void)s; g_cli_interrupted = 1; }

static void reap_children(sv_ctx_t *ctx) {
    int st; pid_t pid;
    while ((pid = waitpid(-1, &st, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->meta_mu);
        for (container_rec_t *c = ctx->containers; c; c = c->next) {
            if (c->host_pid != pid) continue;
            
            if (WIFEXITED(st)) { 
                c->exit_code = WEXITSTATUS(st); 
                c->exit_signal = 0; 
                c->state = CS_EXITED; 
            } else if (WIFSIGNALED(st)) {
                c->exit_signal = WTERMSIG(st); 
                c->exit_code = 128 + c->exit_signal;
                if (c->stop_requested) {
                    c->state = CS_STOPPED;
                } else if (c->exit_signal == SIGKILL) {
                    c->state = CS_KILLED;
                } else {
                    c->state = CS_EXITED;
                }
            }
            if (ctx->monitor_fd >= 0) {
                unreg_monitor(ctx->monitor_fd, c->id, pid);
            }
            break;
        }
        pthread_mutex_unlock(&ctx->meta_mu);
    }
}

static pid_t spawn_container(sv_ctx_t *ctx, const ctrl_req_t *req, container_rec_t *rec) {
    char abs_rootfs[PATH_MAX];
    if (!realpath(req->rootfs, abs_rootfs)) return -1;
    
    struct stat st; 
    if (stat(abs_rootfs, &st) < 0 || !S_ISDIR(st.st_mode)) return -1;

    int log_pipe[2], rdy_pipe[2]; 
    char *stack = NULL; 
    child_cfg_t *cfg = NULL; 
    pid_t pid;
    
    if (pipe(log_pipe) < 0) return -1;
    if (pipe(rdy_pipe) < 0) { 
        close(log_pipe[0]); 
        close(log_pipe[1]); 
        return -1; 
    }

    stack = malloc(STACK_SIZE); 
    cfg = malloc(sizeof(*cfg));
    if (!stack || !cfg) { 
        free(stack); 
        free(cfg); 
        close(log_pipe[0]); 
        close(log_pipe[1]); 
        close(rdy_pipe[0]); 
        close(rdy_pipe[1]); 
        return -1; 
    }

    memset(cfg, 0, sizeof(*cfg)); 
    scopy(cfg->id, req->container_id, CONTAINER_ID_LEN);
    scopy(cfg->rootfs, abs_rootfs, PATH_MAX); 
    scopy(cfg->command, req->command, CMD_LEN);
    cfg->nice_val = req->nice_val; 
    cfg->log_wfd = log_pipe[1]; 
    cfg->ready_wfd= rdy_pipe[1];

    pid = clone(child_fn, stack + STACK_SIZE, CLONE_NEWPID|CLONE_NEWUTS|CLONE_NEWNS|SIGCHLD, cfg);
    if (pid < 0) { 
        free(cfg); 
        free(stack); 
        close(log_pipe[0]); 
        close(log_pipe[1]); 
        close(rdy_pipe[0]); 
        close(rdy_pipe[1]); 
        return -1; 
    }

    close(log_pipe[1]); 
    close(rdy_pipe[1]);
    
    {
        fd_set rfds; 
        FD_ZERO(&rfds); 
        FD_SET(rdy_pipe[0], &rfds); 
        struct timeval tv = { READY_TIMEOUT_SEC, 0 };
        if (select(rdy_pipe[0]+1, &rfds, NULL, NULL, &tv) > 0) { 
            char r; 
            ssize_t rd = read(rdy_pipe[0], &r, 1); 
            (void)rd; 
        }
    }
    close(rdy_pipe[0]); 
    free(cfg); 
    free(stack);

    pthread_mutex_lock(&ctx->meta_mu);
    rec->host_pid = pid; 
    rec->state = CS_RUNNING; 
    rec->started_at = time(NULL);
    snprintf(rec->log_path, PATH_MAX, LOG_DIR "/%s.log", rec->id);
    pthread_mutex_unlock(&ctx->meta_mu);

    if (ctx->monitor_fd >= 0) {
        reg_monitor(ctx->monitor_fd, req->container_id, pid, req->soft_bytes, req->hard_bytes);
    }

    reader_arg_t *ra = malloc(sizeof(*ra));
    if (ra) {
        ra->read_fd = log_pipe[0]; 
        scopy(ra->container_id, req->container_id, CONTAINER_ID_LEN); 
        ra->buf = &ctx->logbuf;
        
        pthread_t tid; 
        if (pthread_create(&tid, NULL, reader_thread, ra) == 0) {
            rpool_add(&ctx->rpool, tid);
        } else { 
            close(log_pipe[0]); 
            free(ra); 
        }
    } else {
        close(log_pipe[0]);
    }

    return pid;
}

static void do_ps(sv_ctx_t *ctx, int fd) {
    ctrl_resp_t resp; 
    memset(&resp, 0, sizeof(resp)); 
    char *buf = resp.message; 
    int cap = (int)sizeof(resp.message)-1, off = 0;
    
    pthread_mutex_lock(&ctx->meta_mu); 
    container_rec_t *c = ctx->containers;
    
    if (!c) {
        off += snprintf(buf+off, (size_t)(cap-off), "No containers.\n");
    } else {
        off += snprintf(buf+off, (size_t)(cap-off), "%-16s %-8s %-10s %-6s %-10s %-10s\n", "ID","PID","STATE","EXIT","SOFT(MiB)","HARD(MiB)");
        off += snprintf(buf+off, (size_t)(cap-off), "%-16s %-8s %-10s %-6s %-10s %-10s\n", "----------------","--------","----------","------","----------","----------");
        for (; c && off < cap-160; c = c->next) {
            off += snprintf(buf+off, (size_t)(cap-off), "%-16s %-8d %-10s %-6d %-10lu %-10lu\n", c->id, (int)c->host_pid, state_str(c->state), c->exit_code, c->soft_bytes>>20, c->hard_bytes>>20);
        }
    }
    pthread_mutex_unlock(&ctx->meta_mu); 
    resp.status = 0; 
    full_write(fd, &resp, sizeof(resp));
}

static void do_logs(sv_ctx_t *ctx, int fd, const ctrl_req_t *req) {
    ctrl_resp_t resp; 
    memset(&resp, 0, sizeof(resp));
    char lpath[PATH_MAX] = "";
    
    pthread_mutex_lock(&ctx->meta_mu); 
    container_rec_t *c = find_container(ctx, req->container_id); 
    if (c) {
        scopy(lpath, c->log_path, PATH_MAX); 
    }
    pthread_mutex_unlock(&ctx->meta_mu);

    if (!c) { 
        resp.status = -1; 
        snprintf(resp.message, sizeof(resp.message), "Container '%s' not found.\n", req->container_id); 
        full_write(fd, &resp, sizeof(resp)); 
        return; 
    }
    
    resp.status = 0; 
    snprintf(resp.message, sizeof(resp.message), "=== %s ===\n", lpath); 
    full_write(fd, &resp, sizeof(resp));

    int lfd = open(lpath, O_RDONLY);
    if (lfd >= 0) { 
        char tmp[4096]; 
        ssize_t n; 
        while ((n = read(lfd, tmp, sizeof(tmp))) > 0) {
            full_write(fd, tmp, (size_t)n); 
        }
        close(lfd); 
    }
}

static void do_stop(sv_ctx_t *ctx, int fd, const ctrl_req_t *req) {
    ctrl_resp_t resp; 
    memset(&resp, 0, sizeof(resp));
    
    pthread_mutex_lock(&ctx->meta_mu); 
    container_rec_t *c = find_container(ctx, req->container_id);
    if (!c || (c->state != CS_RUNNING && c->state != CS_STARTING)) {
        pthread_mutex_unlock(&ctx->meta_mu); 
        resp.status = -1; 
        snprintf(resp.message, sizeof(resp.message), "Container '%s' not running.\n", req->container_id); 
        full_write(fd, &resp, sizeof(resp)); 
        return;
    }
    
    pid_t pid = c->host_pid; 
    c->stop_requested = 1; 
    c->state = CS_STOPPED; 
    pthread_mutex_unlock(&ctx->meta_mu);

    kill(pid, SIGTERM);
    for (int i = 0; i < 20; i++) { 
        usleep(100000); 
        if (kill(pid, 0) < 0 && errno == ESRCH) break; 
    }
    if (kill(pid, 0) == 0) kill(pid, SIGKILL);
    
    resp.status = 0; 
    snprintf(resp.message, sizeof(resp.message), "Container '%s' stopped.\n", req->container_id); 
    full_write(fd, &resp, sizeof(resp));
}

static void do_start_run(sv_ctx_t *ctx, int fd, const ctrl_req_t *req, int blocking) {
    ctrl_resp_t resp; 
    memset(&resp, 0, sizeof(resp));
    
    pthread_mutex_lock(&ctx->meta_mu);
    if (find_container(ctx, req->container_id)) { 
        pthread_mutex_unlock(&ctx->meta_mu); 
        resp.status = -1; 
        snprintf(resp.message, sizeof(resp.message), "ID '%s' already exists.\n", req->container_id); 
        full_write(fd, &resp, sizeof(resp)); 
        return; 
    }
    
    container_rec_t *rec = calloc(1, sizeof(*rec));
    if (!rec) { 
        pthread_mutex_unlock(&ctx->meta_mu); 
        resp.status = -1; 
        snprintf(resp.message,sizeof(resp.message),"Out of memory.\n"); 
        full_write(fd, &resp, sizeof(resp)); 
        return; 
    }
    
    scopy(rec->id, req->container_id, CONTAINER_ID_LEN); 
    rec->soft_bytes = req->soft_bytes; 
    rec->hard_bytes = req->hard_bytes; 
    rec->state = CS_STARTING; 
    prepend_container(ctx, rec);
    pthread_mutex_unlock(&ctx->meta_mu);

    mkdir(LOG_DIR, 0755); 
    pid_t pid = spawn_container(ctx, req, rec);
    
    if (pid < 0) {
        pthread_mutex_lock(&ctx->meta_mu); 
        remove_container(ctx, rec); 
        pthread_mutex_unlock(&ctx->meta_mu); 
        free(rec);
        resp.status = -1; 
        snprintf(resp.message, sizeof(resp.message), "Failed to spawn '%s'.\n", req->container_id); 
        full_write(fd, &resp, sizeof(resp)); 
        return;
    }

    resp.status = 0; 
    snprintf(resp.message, sizeof(resp.message), "Container '%s' started (pid=%d).\n", req->container_id, (int)pid); 
    full_write(fd, &resp, sizeof(resp));
    
    if (!blocking) return;

    while (1) {
        usleep(200000); 
        pthread_mutex_lock(&ctx->meta_mu);
        container_rec_t *c = find_container(ctx, req->container_id);
        int done = (!c || (c->state != CS_RUNNING && c->state != CS_STARTING));
        int ec = c ? c->exit_code : 0; 
        cstate_t st = c ? c->state : CS_EXITED; 
        pthread_mutex_unlock(&ctx->meta_mu);

        if (done || ctx->should_stop) {
            ctrl_resp_t fin; 
            memset(&fin, 0, sizeof(fin)); 
            fin.status = done ? ec : -1;
            snprintf(fin.message, sizeof(fin.message), done ? "Container '%s' exited (state=%s, code=%d).\n" : "Supervisor shutting down.\n", req->container_id, state_str(st), ec);
            full_write(fd, &fin, sizeof(fin)); 
            return;
        }
    }
}

static void *conn_thread(void *arg) {
    conn_arg_t *ca = (conn_arg_t*)arg; 
    sv_ctx_t *ctx = ca->ctx; 
    int fd = ca->conn_fd; 
    free(ca);
    
    ctrl_req_t req; 
    if (full_read(fd, &req, sizeof(req)) < 0) { 
        close(fd); 
        return NULL; 
    }
    
    switch (req.kind) {
        case CMD_START: do_start_run(ctx, fd, &req, 0); break; 
        case CMD_RUN:   do_start_run(ctx, fd, &req, 1); break;
        case CMD_PS:    do_ps(ctx, fd);                 break; 
        case CMD_LOGS:  do_logs(ctx, fd, &req);         break;
        case CMD_STOP:  do_stop(ctx, fd, &req);         break; 
        default: break;
    }
    close(fd); 
    return NULL;
}

static void sv_loop(sv_ctx_t *ctx) {
    while (!ctx->should_stop) {
        fd_set rfds; 
        FD_ZERO(&rfds); 
        FD_SET(ctx->server_fd, &rfds);
        struct timeval tv = {1, 0};
        
        int sel = select(ctx->server_fd+1, &rfds, NULL, NULL, &tv);
        int sel_errno = errno; /* Fix 1: Capture errno before waitpid overwrites it */

        if (g_sigchld) { 
            g_sigchld = 0; 
            reap_children(ctx); 
        }

        if (sel < 0) { 
            if (sel_errno == EINTR) continue; 
            errno = sel_errno; 
            perror("select"); 
            break; 
        }
        if (sel == 0) continue;

        int cfd = accept(ctx->server_fd, NULL, NULL);
        if (cfd < 0) { 
            if (errno == EINTR) continue; 
            perror("accept"); 
            continue; 
        }

        conn_arg_t *ca = malloc(sizeof(*ca)); 
        if (!ca) { 
            close(cfd); 
            continue; 
        }
        ca->ctx = ctx; 
        ca->conn_fd = cfd;

        pthread_t tid; 
        if (pthread_create(&tid, NULL, conn_thread, ca) != 0) { 
            perror("pthread_create conn"); 
            close(cfd); 
            free(ca); 
        } else {
            pthread_detach(tid);
        }
    }
}

static int run_supervisor(const char *rootfs) {
    sv_ctx_t ctx; 
    memset(&ctx, 0, sizeof(ctx)); 
    ctx.server_fd = ctx.monitor_fd = -1; 
    g_ctx = &ctx;
    
    if (pthread_mutex_init(&ctx.meta_mu, NULL)) return 1; 
    if (bbuf_init(&ctx.logbuf)) return 1; 
    rpool_init(&ctx.rpool);
    
    ctx.monitor_fd = open(MONITOR_DEV, O_RDWR); 
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[sv] Warning: %s unavailable\n", MONITOR_DEV);
    }

    mkdir(LOG_DIR, 0755); 
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0); 
    if (ctx.server_fd < 0) return 1;
    
    struct sockaddr_un addr; 
    memset(&addr, 0, sizeof(addr)); 
    addr.sun_family = AF_UNIX; 
    scopy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path)); 
    unlink(CONTROL_PATH);
    
    if (bind(ctx.server_fd,(struct sockaddr*)&addr,sizeof(addr))<0) return 1; 
    if (listen(ctx.server_fd, 32) < 0) return 1;

    struct sigaction sa; 
    memset(&sa, 0, sizeof(sa)); 
    sa.sa_handler = h_sigchld; 
    sigemptyset(&sa.sa_mask); 
    sa.sa_flags = SA_RESTART|SA_NOCLDSTOP; 
    sigaction(SIGCHLD, &sa, NULL);
    
    sa.sa_handler = h_sigterm; 
    sa.sa_flags = 0; 
    sigaction(SIGINT,  &sa, NULL); 
    sigaction(SIGTERM, &sa, NULL);

    if (pthread_create(&ctx.logger_tid, NULL, logging_thread, &ctx)) return 1;
    
    fprintf(stderr, "[sv] Running. rootfs=%s socket=%s\n", rootfs, CONTROL_PATH);
    sv_loop(&ctx);
    fprintf(stderr, "[sv] Shutting down...\n");

    pthread_mutex_lock(&ctx.meta_mu);
    for (container_rec_t *c = ctx.containers; c; c = c->next) {
        if (c->state == CS_RUNNING || c->state == CS_STARTING) { 
            c->stop_requested = 1; 
            kill(c->host_pid, SIGTERM); 
        }
    }
    pthread_mutex_unlock(&ctx.meta_mu); 
    usleep(500000);
    
    pthread_mutex_lock(&ctx.meta_mu);
    for (container_rec_t *c = ctx.containers; c; c = c->next) {
        if (c->state == CS_RUNNING || c->state == CS_STARTING) {
            kill(c->host_pid, SIGKILL);
        }
    }
    pthread_mutex_unlock(&ctx.meta_mu); 
    usleep(200000); 
    reap_children(&ctx);

    rpool_join(&ctx.rpool); 
    bbuf_shutdown(&ctx.logbuf); 
    pthread_join(ctx.logger_tid, NULL); 
    bbuf_destroy(&ctx.logbuf); 
    rpool_destroy(&ctx.rpool);
    
    pthread_mutex_lock(&ctx.meta_mu); 
    for (container_rec_t *c = ctx.containers; c;) { 
        container_rec_t *n = c->next; 
        free(c); 
        c = n; 
    }
    pthread_mutex_unlock(&ctx.meta_mu); 
    pthread_mutex_destroy(&ctx.meta_mu);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd); 
    close(ctx.server_fd); 
    unlink(CONTROL_PATH);
    
    fprintf(stderr, "[sv] Clean exit.\n"); 
    return 0;
}

static int cli_connect(void) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0); 
    if (fd < 0) return -1;
    struct sockaddr_un addr; 
    memset(&addr, 0, sizeof(addr)); 
    addr.sun_family = AF_UNIX; 
    scopy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path));
    if (connect(fd,(struct sockaddr*)&addr,sizeof(addr)) < 0) { 
        close(fd); 
        return -1; 
    }
    return fd;
}

static void send_stop(const char *id) {
    int fd = cli_connect(); 
    if (fd < 0) return;
    ctrl_req_t req; 
    memset(&req, 0, sizeof(req)); 
    req.kind = CMD_STOP; 
    scopy(req.container_id, id, CONTAINER_ID_LEN); 
    full_write(fd, &req, sizeof(req));
    ctrl_resp_t resp; 
    full_read(fd, &resp, sizeof(resp)); 
    close(fd);
}

static int cli_send(const ctrl_req_t *req) {
    int fd = cli_connect(); 
    if (fd < 0) { 
        fprintf(stderr, "Cannot connect to supervisor. Is it running?\n"); 
        return 1; 
    }
    if (full_write(fd, req, sizeof(*req)) < 0) { 
        close(fd); 
        return 1; 
    }
    ctrl_resp_t resp; 
    if (full_read(fd, &resp, sizeof(resp)) < 0) { 
        close(fd); 
        return 1; 
    }
    printf("%s", resp.message); 
    fflush(stdout);
    if (req->kind == CMD_LOGS) { 
        char tmp[4096]; 
        ssize_t n; 
        while ((n = read(fd, tmp, sizeof(tmp))) > 0) {
            fwrite(tmp, 1, (size_t)n, stdout); 
        }
        fflush(stdout); 
    }
    close(fd); 
    return resp.status ? 1 : 0;
}

static int cmd_start(int argc, char *argv[]) {
    if (argc < 5) { usage(argv[0]); return 1; } 
    ctrl_req_t req; 
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START; 
    scopy(req.container_id, argv[2], CONTAINER_ID_LEN); 
    scopy(req.rootfs, argv[3], PATH_MAX); 
    scopy(req.command, argv[4], CMD_LEN);
    req.soft_bytes = DEFAULT_SOFT; 
    req.hard_bytes = DEFAULT_HARD; 
    if (parse_opts(&req, argc, argv, 5)) return 1; 
    return cli_send(&req);
}

static int cmd_run(int argc, char *argv[]) {
    if (argc < 5) { usage(argv[0]); return 1; } 
    ctrl_req_t req; 
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN; 
    scopy(req.container_id, argv[2], CONTAINER_ID_LEN); 
    scopy(req.rootfs, argv[3], PATH_MAX); 
    scopy(req.command, argv[4], CMD_LEN);
    req.soft_bytes = DEFAULT_SOFT; 
    req.hard_bytes = DEFAULT_HARD; 
    if (parse_opts(&req, argc, argv, 5)) return 1;
    
    scopy(g_cli_run_id, req.container_id, CONTAINER_ID_LEN); 
    g_cli_interrupted = 0;
    
    struct sigaction sa; 
    memset(&sa, 0, sizeof(sa)); 
    sa.sa_handler = h_sigcli; 
    sigemptyset(&sa.sa_mask); 
    sa.sa_flags = 0; 
    sigaction(SIGINT,  &sa, NULL); 
    sigaction(SIGTERM, &sa, NULL);
    
    int fd = cli_connect(); 
    if (fd < 0) { 
        fprintf(stderr, "Cannot connect to supervisor. Is it running?\n"); 
        return 1; 
    }
    if (full_write(fd, &req, sizeof(req)) < 0) { 
        close(fd); 
        return 1; 
    }
    
    ctrl_resp_t resp; 
    if (full_read(fd, &resp, sizeof(resp)) < 0) { 
        close(fd); 
        return 1; 
    }
    printf("%s", resp.message); 
    fflush(stdout); 
    if (resp.status) { 
        close(fd); 
        return 1; 
    }
    
    int stop_sent = 0, ec = 1;
    while (1) {
        fd_set rfds; 
        FD_ZERO(&rfds); 
        FD_SET(fd, &rfds); 
        struct timeval tv = {0, 200000};
        
        int sel = select(fd+1, &rfds, NULL, NULL, &tv); 
        if (sel < 0 && errno != EINTR) break;
        
        if (g_cli_interrupted && !stop_sent) { 
            fprintf(stderr,"\n[run] Interrupted – forwarding stop\n"); 
            send_stop(g_cli_run_id); 
            stop_sent = 1; 
        }
        if (sel > 0) { 
            ctrl_resp_t fin; 
            if (full_read(fd, &fin, sizeof(fin)) < 0) break; 
            printf("%s", fin.message); 
            fflush(stdout); 
            ec = fin.status ? fin.status : 0; 
            break; 
        }
    }
    close(fd); 
    return ec;
}

static int cmd_ps(void) { 
    ctrl_req_t req; 
    memset(&req,0,sizeof(req)); 
    req.kind=CMD_PS; 
    return cli_send(&req); 
}

static int cmd_logs(int argc, char *argv[]) {
    if (argc < 3) return 1; 
    ctrl_req_t req; 
    memset(&req,0,sizeof(req)); 
    req.kind = CMD_LOGS; 
    scopy(req.container_id, argv[2], CONTAINER_ID_LEN); 
    return cli_send(&req);
}

static int cmd_stop(int argc, char *argv[]) {
    if (argc < 3) return 1; 
    ctrl_req_t req; 
    memset(&req,0,sizeof(req)); 
    req.kind = CMD_STOP; 
    scopy(req.container_id, argv[2], CONTAINER_ID_LEN); 
    return cli_send(&req);
}

int main(int argc, char *argv[]) {
    if (argc < 2) { 
        usage(argv[0]); 
        return 1; 
    }
    if (!strcmp(argv[1],"supervisor")) { 
        if (argc < 3) return 1; 
        return run_supervisor(argv[2]); 
    }
    if (!strcmp(argv[1],"start")) return cmd_start(argc, argv); 
    if (!strcmp(argv[1],"run"))   return cmd_run(argc, argv);
    if (!strcmp(argv[1],"ps"))    return cmd_ps();              
    if (!strcmp(argv[1],"logs"))  return cmd_logs(argc, argv);
    if (!strcmp(argv[1],"stop"))  return cmd_stop(argc, argv);  
    
    usage(argv[0]); 
    return 1;
}
