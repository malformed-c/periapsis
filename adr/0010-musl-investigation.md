# ADR-0010 Appendix: Musl libc Investigation

**Date:** 2026-04-12

---

## Problem

`systemd-nspawn --user=<non-root>` fails on musl-based (Alpine/BusyBox) container images with:

```
Failed to resolve user <user>.
```

This affects **all** non-root users — both injected (`peri-1000`) and image-native (`nginx`, `mail`, `nobody`).

glibc-based images (Debian, Ubuntu) are unaffected.

---

## Root Cause

systemd-nspawn v260 resolves `--user=` by forking into the container namespace and running:

1. `getent passwd <user>` — lookup user entry
2. `getent initgroups <user>` — lookup supplementary groups

musl libc's `getent` does not support the `initgroups` database:

```console
# Inside Alpine container:
$ getent initgroups nginx
getent: Unknown database `initgroups'
```

glibc's `getent` supports it natively, which is why glibc images work.

---

## Investigation Steps

### 1. Confirm the failure

```bash
# Create a minimal Alpine rootfs (or use an extracted nginx:alpine image)
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=nginx --console=pipe -- /bin/true
# Output: Failed to resolve user nginx.

# Same failure with numeric UID
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=101 --console=pipe -- /bin/true
# Output: Failed to resolve user 101.

# Root works fine (nspawn skips getent for UID 0)
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=root --console=pipe -- /bin/true
# Success
```

### 2. Confirm glibc works

```bash
# Using a Debian/Ubuntu rootfs
sudo systemd-nspawn --directory=/tmp/nspawn-rootfs --user=nobody --console=pipe -- /bin/true
# Success
```

### 3. Identify how nspawn finds getent

```bash
strings /usr/bin/systemd-nspawn | grep -i getent
# Output:
#   getent passwd
#   getent initgroups
#   spawn_getent

# Check PATH used by nspawn's internal fork:
strings /usr/bin/systemd-nspawn | grep "^PATH="
# Output: PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
```

Key finding: nspawn uses PATH resolution (not a hardcoded path), and `/usr/local/bin` comes before `/usr/bin`.

### 4. Confirm the execution environment is restricted

Early shim attempts using `grep`, `cat`, `ls` all failed — nspawn's `spawn_getent` fork has an extremely minimal execution environment where only the shell and its builtins are available:

```bash
# Debug shim that tries external commands:
sudo bash -c 'cat > /tmp/nspawn-nginx/usr/local/bin/getent << "EOF"
#!/bin/sh
echo "$@" >> /var/getent-debug.log
grep "test" /etc/passwd >> /var/getent-debug.log 2>&1
exec /usr/bin/getent "$@"
EOF
chmod 755 /tmp/nspawn-nginx/usr/local/bin/getent'

sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=nginx --console=pipe -- /bin/true

# Debug log shows:
#   grep: not found
#   cat: not found
#   ls: not found
```

### 5. Write a builtins-only shim

The shim must use only POSIX shell builtins (`read`, `case`, `printf`, `while`, `[`):

```bash
sudo bash -c 'cat > /tmp/nspawn-nginx/usr/local/bin/getent << '\''EOF'\''
#!/bin/sh
if [ "$1" = "initgroups" ]; then
    user="$2"
    [ -z "$user" ] && exit 1
    resolved=""
    while IFS=: read -r name x uid gid gecos home shell; do
        if [ "$name" = "$user" ] || [ "$uid" = "$user" ]; then
            resolved="$name"
            break
        fi
    done < /etc/passwd
    [ -z "$resolved" ] && exit 2
    groups=""
    while IFS=: read -r gname x gid members; do
        rest=",$members,"
        case "$rest" in
            *",$resolved,"*) groups="$groups $gid" ;;
        esac
    done < /etc/group
    printf "%s%s\n" "$resolved" "$groups"
    exit 0
fi
for p in /usr/bin/getent /bin/getent /usr/sbin/getent; do
    [ -x "$p" ] && [ "$p" != "$0" ] && exec "$p" "$@"
done
exit 1
EOF
chmod 755 /tmp/nspawn-nginx/usr/local/bin/getent'
```

### 6. Verify the shim works

```bash
# By username
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=nginx --console=pipe -- /bin/true
# Success

# By numeric UID
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=101 --console=pipe -- /bin/true
# Success

# Other users
sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=mail --console=pipe -- /bin/true
# Success

sudo systemd-nspawn --directory=/tmp/nspawn-nginx --user=8 --console=pipe -- /bin/true
# Success
```

### 7. Verify with user namespace isolation

```bash
sudo systemd-nspawn \
    --directory=/tmp/nspawn-nginx \
    --private-users=131072:65536 \
    --private-users-ownership=chown \
    --user=101 \
    --console=pipe \
    -- /bin/true
# Success
```

---

## Detection

musl-based rootfs images are identified by the presence of `/lib/ld-musl-*.so.*` (the musl dynamic linker):

```bash
ls /tmp/nspawn-nginx/lib/ld-musl-*
# /tmp/nspawn-nginx/lib/ld-musl-x86_64.so.1   -> musl

ls /tmp/nspawn-rootfs/lib/ld-linux-*
# /tmp/nspawn-rootfs/lib/ld-linux-x86-64.so.2  -> glibc
```

---

## Solution

Implemented in `internal/runtime/systemd/musl.go`:

1. **`isMuslRootFS(rootfs)`** — globs for `/lib/ld-musl-*.so.*` in the rootfs
2. **`ensureGetentShim(rootfs, logger)`** — if musl detected, logs a warning and writes the builtins-only getent shim to `<rootfs>/usr/local/bin/getent`
3. **Called from `RunMachine`** before `--user=` is appended, only for non-root UIDs (root skips getent resolution)

The shim:
- Handles `getent initgroups <user>` using `while IFS=: read` to parse `/etc/passwd` and `/etc/group`
- Supports both username and numeric UID lookups
- Delegates all other databases (`passwd`, `group`, `hosts`, etc.) to the real `/usr/bin/getent`
- Uses zero external commands — only POSIX shell builtins

---

## Scope

- Affects: Alpine, BusyBox, and any musl-based container image
- Does not affect: Debian, Ubuntu, Fedora, or any glibc-based image
- nspawn version tested: systemd v260
- The shim is idempotent — safe to overwrite on repeated container starts
