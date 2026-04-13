/*
 * userns-shim — user namespace setup shim for perigeos containers.
 *
 * Runs as the container entrypoint inside nspawn (as root, no --user=).
 * Calls unshare(CLONE_NEWUSER), signals the host via FIFO, waits for
 * perigeos to write uid_map/gid_map and send the target identity via
 * the gate pipe, then exec()s the real workload.
 *
 * Protocol:
 *   1. Shim calls unshare(CLONE_NEWUSER)
 *   2. Shim writes "1\n" to /run/userns/ready  (FIFO)
 *   3. Host writes /proc/<pid>/uid_map, setgroups "deny", gid_map
 *   4. Host writes "<uid>:<gid>\n" to /run/userns/gate (FIFO)
 *   5. Shim parses target, calls setgid()+setuid()
 *   6. Shim exec()s argv[1:]
 *
 * Build:
 *   cc -static -o userns-shim main.c
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define READY_FIFO "/run/userns/ready"
#define GATE_FIFO  "/run/userns/gate"
#define TIMEOUT_MS 30000

static void die(const char *msg)
{
	perror(msg);
	_exit(1);
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		fprintf(stderr, "usage: userns-shim <command> [args...]\n");
		return 1;
	}

	/* Enter new user namespace. Process becomes uid 65534 (unmapped). */
	if (unshare(CLONE_NEWUSER) < 0)
		die("unshare(CLONE_NEWUSER)");

	/* Signal host: uid_map can be written now. */
	int rfd = open(READY_FIFO, O_WRONLY);
	if (rfd < 0)
		die("open " READY_FIFO);
	if (write(rfd, "1\n", 2) < 0)
		die("write ready");
	close(rfd);

	/* Wait for host to send target identity via gate pipe. */
	int gfd = open(GATE_FIFO, O_RDONLY);
	if (gfd < 0)
		die("open " GATE_FIFO);

	struct pollfd pfd = {.fd = gfd, .events = POLLIN};
	int ret = poll(&pfd, 1, TIMEOUT_MS);
	if (ret == 0) {
		fprintf(stderr, "userns-shim: timed out waiting for uid_map\n");
		_exit(1);
	}
	if (ret < 0)
		die("poll");

	/* Read "uid:gid\n" from gate. */
	char buf[32];
	ssize_t n = read(gfd, buf, sizeof(buf) - 1);
	close(gfd);
	if (n <= 0)
		die("read gate");
	buf[n] = '\0';

	uid_t target_uid = 0;
	gid_t target_gid = 0;
	if (sscanf(buf, "%u:%u", &target_uid, &target_gid) != 2) {
		fprintf(stderr, "userns-shim: bad gate payload: %s\n", buf);
		_exit(1);
	}

	/*
	 * Adopt the mapped identity. Host wrote setgroups "deny" so
	 * supplementary groups cannot be re-added.
	 * Order: gid → uid. Once uid is non-zero we lose CAP_SETGID.
	 */
	if (setgid(target_gid) < 0)
		die("setgid");
	if (setuid(target_uid) < 0)
		die("setuid");

	execvp(argv[1], &argv[1]);
	die("execvp");
}
