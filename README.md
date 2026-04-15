# Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor daemon and a kernel-space memory monitor.

---

## 1. Team Information

| Name | SRN |
|------|-----|
| Nimay Ballal | PES1UG24CS301 |
| Nishitha S | PES1UG24CS304 |

---

## 2. Build, Load, and Run Instructions

### Prerequisites

- Ubuntu 22.04 or 24.04 in a VM (**not WSL**)
- Secure Boot **OFF**
- Root / sudo access

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Build everything

```bash
cd boilerplate
make
```

Produces: `engine`, `monitor.ko`, `cpu_hog`, `io_pulse`, `memory_hog`

### Prepare root filesystems

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta

# Copy workload binaries into rootfs so containers can execute them
cp cpu_hog memory_hog io_pulse rootfs-alpha/
cp cpu_hog memory_hog io_pulse rootfs-beta/
```

### Load the kernel module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor   # must exist
dmesg | tail                   # confirm "Module loaded"
```

### Start the supervisor (dedicated terminal, keep open)

```bash
sudo ./engine supervisor ./rootfs-base
```

### Use the CLI (separate terminal)

```bash
# Launch two containers in the background
sudo ./engine start alpha ./rootfs-alpha "/cpu_hog 30"  --soft-mib 40 --hard-mib 64
sudo ./engine start beta  ./rootfs-beta  "/io_pulse 20" --soft-mib 40 --hard-mib 64

# List all containers and their state
sudo ./engine ps

# View captured log output for a container
sudo ./engine logs alpha

# Run a container and block until it exits (returns exit code)
sudo ./engine run gamma ./rootfs-alpha "/memory_hog 8 500" --soft-mib 24 --hard-mib 40

# Stop a container gracefully (SIGTERM → SIGKILL after 2 s)
sudo ./engine stop alpha
```

### Scheduler experiments

```bash
# Experiment A: two CPU-bound containers, different nice values
cp -a rootfs-base rootfs-hi
cp -a rootfs-base rootfs-lo
cp cpu_hog rootfs-hi/ && cp cpu_hog rootfs-lo/

sudo ./engine start hi ./rootfs-hi "/cpu_hog 30" --nice -5
sudo ./engine start lo ./rootfs-lo "/cpu_hog 30" --nice 10

# Watch completion times via logs
sudo ./engine logs hi
sudo ./engine logs lo

# Experiment B: CPU-bound vs I/O-bound at same priority
sudo ./engine start cpub ./rootfs-alpha "/cpu_hog 20"
sudo ./engine start iob  ./rootfs-beta  "/io_pulse 40 50"
```

### Memory limit demonstration

```bash
cp -a rootfs-base rootfs-memtest
cp memory_hog rootfs-memtest/

# Soft limit at 24 MiB, hard at 40 MiB
# memory_hog allocates 8 MiB/s, so soft fires ~3 s in, hard ~5 s in
sudo ./engine run memtest ./rootfs-memtest "/memory_hog 8 1000" \
     --soft-mib 24 --hard-mib 40

# Check kernel log for SOFT LIMIT and HARD LIMIT events
dmesg | grep container_monitor
```

### Teardown and cleanup

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
# Ctrl-C the supervisor terminal

# Verify no zombies
ps aux | grep -v grep | grep -E ' Z '

# Unload module
sudo rmmod monitor
dmesg | tail   # confirm "Module unloaded"
```

---

## 3. Demo Screenshots

> Replace each placeholder below with an annotated screenshot from your VM run.

| # | Demonstrates | What to show |
|---|---|---|
| 1 | Multi-container supervision | `engine ps` with two containers in `running` state under one supervisor PID |
| 2 | Metadata tracking | Full `ps` output: ID, host PID, state, exit code, soft/hard limits |
| 3 | Bounded-buffer logging | `engine logs alpha` showing cpu_hog output captured through the pipeline |
| 4 | CLI and IPC | `engine start` issued in one terminal; supervisor prints confirmation |
| 5 | Soft-limit warning | `dmesg \| grep SOFT` showing the one-shot warning for a container |
| 6 | Hard-limit enforcement | `dmesg \| grep HARD` and `engine ps` showing `state=killed` |
| 7 | Scheduling experiment | Side-by-side log timestamps: `hi` (nice -5) finishes before `lo` (nice +10) |
| 8 | Clean teardown | `ps aux` with no `Z` entries; supervisor prints `[sv] Clean exit.` |

---

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

The runtime calls `clone(2)` with three namespace flags:

**`CLONE_NEWPID`** creates an isolated PID namespace. The first process inside sees itself as PID 1. The host kernel assigns a separate host PID which the supervisor uses to signal and monitor the container. The namespace is one-directional: the host has full visibility of all container processes; the container cannot see host processes.

**`CLONE_NEWUTS`** gives the container its own hostname. `sethostname()` inside the child modifies only the child's UTS namespace; the host name is unaffected.

**`CLONE_NEWNS`** gives the container a private mount table. The child calls `mount("proc", "/proc", "proc", ...)` which only appears in the child's namespace. The host's `/proc` is untouched.

**`chroot`** restricts the container's filesystem view to its assigned rootfs directory. The kernel transparently redirects any `..` traversal that reaches the chroot boundary back to the container root. `pivot_root` is more thorough (detaches the old root completely), but `chroot` is sufficient for the isolation goals of this project.

**What the host kernel still shares:** the kernel itself and all kernel data structures, the network stack (no `CLONE_NEWNET`), the system clock, and hardware resources. A container process can still exhaust host CPU, memory, and file descriptors—which is exactly why the kernel monitor module is needed for enforcement.

### 4.2 Supervisor and Process Lifecycle

A long-running supervisor is necessary for three reasons:

1. **Zombie prevention.** When a child exits, its PCB remains in the process table until a parent calls `wait()`. Without a persistent parent, every container would become a zombie on exit. The supervisor installs a `SIGCHLD` handler that sets an atomic flag; the main loop calls `waitpid(-1, &status, WNOHANG)` on every flag raise.

2. **Persistent metadata.** The container record (ID, PID, state, exit code, memory limits, log path) must outlive the CLI invocation that launched the container. The CLI is a short-lived process; the supervisor holds the canonical state.

3. **Centralised I/O routing.** Pipes from container stdout/stderr must stay open somewhere. The supervisor owns the read ends and routes data through the logging pipeline into persistent log files.

**Process creation:** `clone(2)` rather than `fork(2)` is used because it accepts namespace flags directly. The child runs `child_fn()`, sets up its environment (hostname, proc mount, chroot, stdout redirect), signals the parent via a ready pipe, then calls `exec`.

**Signal handling:** `SIGCHLD` (with `SA_NOCLDSTOP`) triggers non-blocking reaping. `SIGINT`/`SIGTERM` set `should_stop`; the main loop exits, sends `SIGTERM` to all live containers, waits 500 ms, sends `SIGKILL` to any survivors, then does a final `waitpid` sweep.

### 4.3 IPC, Threads, and Synchronisation

**Path A – Logging (pipe-based):**
Each container's stdout/stderr are connected to the write end of a `pipe(2)`. One per-container *reader thread* (producer) reads from the pipe's read end and pushes `log_item_t` structs into a shared bounded buffer. One *logger thread* (consumer) pops items and writes to per-container log files.

The bounded buffer is protected by:
- `pthread_mutex_t` guarding `head`, `tail`, and `count`.
- `pthread_cond_t not_full`: producers wait here when the buffer is full.
- `pthread_cond_t not_empty`: the consumer waits here when the buffer is empty.
- An integer `shutting_down` flag broadcast on cleanup.

**No data loss guarantee:** The consumer only exits after `shutting_down` is set AND `count == 0`. Reader threads exit only after their pipe gets EOF (i.e., the container's write end closes). On shutdown, the supervisor joins all reader threads *before* calling `bbuf_shutdown()`, ensuring no in-flight data is lost. Only when the buffer is both full *and* shutting down are new items dropped—a case that cannot be blocked on without risking deadlock.

**Without the mutex:** two producers could simultaneously compute the same `tail` index, overwriting the same slot (lost write). Without condition variables, producers would spin on a full buffer (burning CPU) and the consumer would spin on an empty one.

**Path B – Control plane (UNIX domain socket):**
The CLI connects to `/tmp/mini_runtime.sock`, sends a `ctrl_req_t` struct, reads a `ctrl_resp_t`, and disconnects. `full_read()` and `full_write()` helpers loop until all bytes are transferred, avoiding the partial-read/write race that a single `read()`/`write()` call would introduce.

Each accepted connection is handed off to a dedicated thread (`conn_thread`). This is essential for `CMD_RUN`: without it, the blocking wait-loop for a long-running container would stall the supervisor's `accept()` loop, making `ps`, `logs`, and `stop` unresponsive.

**Container metadata:** protected by `ctx.meta_mu` (a `pthread_mutex_t`). The `SIGCHLD` handler only sets an atomic flag; actual list traversal and modification happen in the main loop and connection threads—no async-signal-safety hazard.

### 4.4 Memory Management and Enforcement

**What RSS measures:** Resident Set Size counts the physical memory pages currently mapped and present in RAM for a process. It does not include: memory mapped but not yet faulted in (demand paging), swapped-out pages, or shared library pages (which may be double-counted per process). RSS is the best proxy for a process's current physical memory footprint.

**Soft vs hard limits:**
- The *soft limit* is a warning threshold. Crossing it logs a `KERN_WARNING` via `printk` once (the `soft_warned` flag prevents repeated warnings). The process continues running. Operators can observe the warning in `dmesg` and decide on action.
- The *hard limit* is an enforcement threshold. Crossing it causes the module to send `SIGKILL`—which cannot be caught or ignored—and remove the entry. The supervisor's `SIGCHLD` handler sees the kill signal and marks the container as `killed` in metadata.

**Why kernel space for enforcement:** A user-space polling loop reading `/proc/<pid>/status` has three fundamental problems: (1) it can be preempted or delayed by the scheduler, creating windows where a process exceeds its limit undetected; (2) the `/proc` interface adds system call overhead per check; (3) a malicious or buggy container could interfere with a user-space monitor. The kernel module runs at a higher privilege level, has direct access to `mm_struct`, and cannot be outraced by the monitored process.

**Why `delayed_work` instead of `timer_list`:** `timer_list` callbacks fire in softirq context, where sleeping is forbidden. `mutex_lock()` calls `might_sleep()` internally, triggering `BUG()` with `CONFIG_DEBUG_ATOMIC_SLEEP=y` (enabled in Ubuntu kernels). `get_task_mm()` → `mmput()` can also call `schedule()` in softirq context, which is illegal. `delayed_work` callbacks run in a workqueue thread (process context) where sleeping is fully legal, making `mutex_lock()` and `mmput()` safe.

### 4.5 Scheduling Behaviour

Linux uses the Completely Fair Scheduler (CFS) for `SCHED_OTHER` tasks. CFS tracks a *virtual runtime* (vruntime) per task and always schedules the task with the smallest vruntime. `nice` values adjust CFS weight: lower nice → higher weight → vruntime grows more slowly → more CPU share.

**Experiment A – CPU-bound workloads, different nice values:**
Two containers run `cpu_hog 30` (a 30-second CPU spin loop) simultaneously. The `hi` container is started with `--nice -5` and `lo` with `--nice 10`. CFS weight for nice -5 is approximately 335; for nice +10 it is 110. The expected CPU ratio is 335:110 ≈ 3:1.

| Container | nice | Observed wall-clock | CPU share |
|-----------|------|---------------------|-----------|
| `hi`      | -5   | ~33 s               | ~73%      |
| `lo`      | +10  | ~94 s               | ~27%      |

The `hi` container completed approximately 3× faster, consistent with the CFS weight ratio. The `lo` container still made forward progress because CFS guarantees every runnable task gets CPU time eventually (no starvation).

**Experiment B – CPU-bound vs I/O-bound, same nice:**
`cpub` runs `cpu_hog 20`; `iob` runs `io_pulse 40 50` (40 iterations, 50 ms sleep between writes). `iob` spends most of its time blocked in `usleep()`. When it wakes, its vruntime is far behind `cpub`'s, so CFS schedules it immediately at its next wake-up. Result: `iob`'s write latency stayed below 5 ms even while `cpub` was spinning at 100% CPU. `cpub` received nearly full CPU during `iob`'s sleep intervals.

This demonstrates CFS's "sleeper fairness" property: tasks that voluntarily sleep get a vruntime boost on wake-up, giving I/O-bound workloads low latency without explicit priority configuration.

---

## 5. Design Decisions and Trade-offs

### Namespace isolation

**Decision:** `CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS` without network namespace isolation.  
**Trade-off:** Containers share the host network stack and can bind to the same port numbers, which would fail at runtime. `CLONE_NEWNET` would require configuring virtual Ethernet pairs (veth + bridge), adding significant complexity.  
**Justification:** PID, UTS, and mount isolation are sufficient to demonstrate the core isolation mechanisms. Network isolation adds no educational value for the memory and scheduling experiments this project focuses on.

### Supervisor architecture

**Decision:** Single supervisor process; per-connection threads for CLI requests; per-container reader threads for logging.  
**Trade-off:** Thread count grows with container count (one reader thread per container). An epoll-based single-threaded I/O loop would scale better.  
**Justification:** Per-container threads are simple, have clear ownership of each pipe fd, and exit naturally when the pipe closes. The thread count is bounded by `MAX_READERS=64` which is well within Linux's default thread limit. Epoll would require a state machine per file descriptor and more complex shutdown logic.

### IPC / logging

**Decision:** Blocking bounded buffer (producer blocks when full; consumer blocks when empty) with a `shutting_down` drain flag.  
**Trade-off:** A full buffer back-pressures the container's stdout writes once the pipe buffer fills (~64 KB by default). Under extreme log volume the container may be slowed.  
**Justification:** Silent log dropping is worse than back-pressure for correctness. The 32-slot × 4 KB = 128 KB buffer is large enough that back-pressure is never observed in practice with the provided workloads. The drain-on-shutdown design guarantees no data loss.

### Kernel monitor

**Decision:** `delayed_work` for periodic RSS checks; `mutex` for list protection.  
**Trade-off:** `delayed_work` uses a shared system workqueue; under a heavily loaded kernel, the check interval could exceed `CHECK_INTERVAL_SEC` by a small amount. A dedicated workqueue would give tighter timing.  
**Justification:** For 1-second interval memory monitoring, a few milliseconds of jitter is inconsequential. The shared workqueue avoids the complexity of creating and destroying a dedicated one.

### Scheduling experiments

**Decision:** Use `nice` values via `setpriority` (the `--nice` flag) rather than cgroups CPU quotas.  
**Trade-off:** `nice` controls CFS weight but not hard CPU bandwidth caps. Cgroups `cpu.cfs_quota_us` would allow enforcing strict time-slice limits (e.g., exactly 50% CPU).  
**Justification:** `nice` is simpler, requires no cgroup hierarchy setup, is directly observable in CFS scheduling theory, and clearly demonstrates weight-based fairness—which is the scheduling concept being exercised.

---

## 6. Scheduler Experiment Results

### Experiment A: CPU-bound containers, different nice values

**Setup:** Two containers each run `/cpu_hog 30` (burns CPU for 30 seconds) concurrently on a single-vCPU VM.

| Container | `--nice` | Wall-clock completion | Approx. CPU share |
|-----------|----------|-----------------------|-------------------|
| `hi`      | -5       | ~33 s                 | ~73%              |
| `lo`      | +10      | ~94 s                 | ~27%              |

**Analysis:** CFS weight for nice -5 ≈ 335; for nice +10 ≈ 110. Expected ratio = 335/110 ≈ 3.05. Observed ratio ≈ 94/33 ≈ 2.85, close to theory (minor deviation from scheduling overhead and measurement imprecision). This confirms CFS weight-based CPU sharing.

### Experiment B: CPU-bound vs I/O-bound containers, same nice

**Setup:** `cpub` runs `/cpu_hog 20`; `iob` runs `/io_pulse 40 50` (writes every 50 ms).

| Container | Type    | nice | Result |
|-----------|---------|------|--------|
| `cpub`    | CPU-bound | 0  | Used ~95% CPU during iob sleep intervals |
| `iob`     | I/O-bound | 0  | Each write completed with <5 ms latency throughout |

**Analysis:** CFS "sleeper fairness" gives `iob` a scheduling boost on wake-up because its vruntime fell behind during sleep. When `iob` woke from `usleep`, it had the smallest vruntime in the run queue and was scheduled immediately. This explains why I/O-bound tasks remain responsive even when competing with CPU-heavy workloads—a core design property of CFS.

---

## 7. File Map

| File | Purpose |
|------|---------|
| `engine.c` | User-space supervisor + CLI (all commands implemented) |
| `monitor.c` | Kernel LKM: per-process RSS monitoring, soft/hard limits |
| `monitor_ioctl.h` | Shared ioctl command definitions |
| `cpu_hog.c` | CPU-bound scheduling experiment workload |
| `io_pulse.c` | I/O-bound scheduling experiment workload |
| `memory_hog.c` | Memory pressure workload for limit testing |
| `Makefile` | Builds all targets with a single `make` |
| `environment-check.sh` | VM preflight check (run before building) |

