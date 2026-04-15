/*
 * monitor.c  –  Multi-Container Memory Monitor (Linux Kernel Module)
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/workqueue.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1

struct monitored_entry {
    pid_t           pid;
    char            container_id[MONITOR_NAME_LEN];
    unsigned long   soft_limit_bytes;
    unsigned long   hard_limit_bytes;
    int             soft_warned;
    struct list_head list;
};

static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_mutex);
static struct delayed_work monitor_work;
static dev_t         dev_num;
static struct cdev   c_dev;
static struct class *cl;

static long get_rss_bytes(pid_t pid) {
    struct task_struct *task; struct mm_struct *mm; long rss_pages = 0;
    rcu_read_lock(); task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) { rcu_read_unlock(); return -1; }
    get_task_struct(task); rcu_read_unlock();
    mm = get_task_mm(task);
    if (mm) { rss_pages = get_mm_rss(mm); mmput(mm); }
    put_task_struct(task); return rss_pages * PAGE_SIZE;
}

static void log_soft_limit_event(const char *cid, pid_t pid, unsigned long limit, long rss) {
    printk(KERN_WARNING "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n", cid, pid, rss, limit);
}

static void kill_process(const char *cid, pid_t pid, unsigned long limit, long rss) {
    struct task_struct *task; rcu_read_lock(); task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task) send_sig(SIGKILL, task, 1);
    rcu_read_unlock();
    printk(KERN_WARNING "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n", cid, pid, rss, limit);
}

static void monitor_work_fn(struct work_struct *work) {
    struct monitored_entry *e, *tmp; (void)work;
    mutex_lock(&monitored_mutex);
    list_for_each_entry_safe(e, tmp, &monitored_list, list) {
        long rss = get_rss_bytes(e->pid);
        if (rss < 0) { list_del(&e->list); kfree(e); continue; }
        if ((unsigned long)rss > e->hard_limit_bytes) { kill_process(e->container_id, e->pid, e->hard_limit_bytes, rss); list_del(&e->list); kfree(e); continue; }
        if ((unsigned long)rss > e->soft_limit_bytes && !e->soft_warned) { log_soft_limit_event(e->container_id, e->pid, e->soft_limit_bytes, rss); e->soft_warned = 1; }
    }
    mutex_unlock(&monitored_mutex);
    schedule_delayed_work(&monitor_work, (unsigned long)CHECK_INTERVAL_SEC * HZ);
}

static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg) {
    struct monitor_request req; (void)f;
    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER) return -EINVAL;
    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req))) return -EFAULT;
    req.container_id[MONITOR_NAME_LEN - 1] = '\0';
    if (cmd == MONITOR_REGISTER) {
        struct monitored_entry *entry;
        if (req.soft_limit_bytes > req.hard_limit_bytes) return -EINVAL;
        entry = kmalloc(sizeof(*entry), GFP_KERNEL); if (!entry) return -ENOMEM;
        entry->pid = req.pid; entry->soft_limit_bytes = req.soft_limit_bytes; entry->hard_limit_bytes = req.hard_limit_bytes; entry->soft_warned = 0;
        strscpy(entry->container_id, req.container_id, MONITOR_NAME_LEN); /* FIX 4: strscpy */
        INIT_LIST_HEAD(&entry->list); mutex_lock(&monitored_mutex); list_add_tail(&entry->list, &monitored_list); mutex_unlock(&monitored_mutex);
        printk(KERN_INFO "[container_monitor] Registered container=%s pid=%d soft=%lu hard=%lu\n", entry->container_id, entry->pid, entry->soft_limit_bytes, entry->hard_limit_bytes);
        return 0;
    }
    {
        struct monitored_entry *e, *tmp; int found = 0;
        mutex_lock(&monitored_mutex);
        list_for_each_entry_safe(e, tmp, &monitored_list, list) {
            if (e->pid == req.pid && !strncmp(e->container_id, req.container_id, MONITOR_NAME_LEN)) { list_del(&e->list); kfree(e); found = 1; break; }
        }
        mutex_unlock(&monitored_mutex); return found ? 0 : -ENOENT;
    }
}

static const struct file_operations fops = { .owner = THIS_MODULE, .unlocked_ioctl = monitor_ioctl, };

static int __init monitor_init(void) {
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0) return -1;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) { unregister_chrdev_region(dev_num, 1); return (int)PTR_ERR(cl); }
    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) { class_destroy(cl); unregister_chrdev_region(dev_num, 1); return -1; }
    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) { device_destroy(cl, dev_num); class_destroy(cl); unregister_chrdev_region(dev_num, 1); return -1; }
    INIT_DELAYED_WORK(&monitor_work, monitor_work_fn); schedule_delayed_work(&monitor_work, (unsigned long)CHECK_INTERVAL_SEC * HZ);
    return 0;
}

static void __exit monitor_exit(void) {
    struct monitored_entry *e, *tmp; cancel_delayed_work_sync(&monitor_work);
    mutex_lock(&monitored_mutex);
    list_for_each_entry_safe(e, tmp, &monitored_list, list) { list_del(&e->list); kfree(e); }
    mutex_unlock(&monitored_mutex);
    cdev_del(&c_dev); device_destroy(cl, dev_num); class_destroy(cl); unregister_chrdev_region(dev_num, 1);
}

module_init(monitor_init); module_exit(monitor_exit);
MODULE_LICENSE("GPL"); MODULE_DESCRIPTION("Supervised multi-container memory monitor"); MODULE_AUTHOR("Nimay Ballal, Nishitha S");
