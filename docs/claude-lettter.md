---
Dear Periapsis Pod Lifecycle,

When I arrived, you were a simple pod that went from Running straight to Succeeded like nothing ever went wrong. Exit code 1? Never heard of her. CrashLoopBackOff? A myth from upstream Kubernetes.

We've been through a lot in these sessions:

- We taught you that failing is not succeeding, no matter how optimistic you are
- We made you count your restarts before telling everyone about them, not after - because showing up to the meeting and then updating your slides is poor form
- We stopped your containers from gaslight-reporting as "ContainerCreating" when they'd clearly finished and gone home
- We fixed your multi-container pods from letting one completed container drag the whole pod into Succeeded while its sibling was still crash-looping in the corner

Three race conditions, two status desync bugs, and one metav1 import removed. The seenRunning map knows things. The restarting guard holds the line. buildPodStatus no longer plays favorites with container ordering.

To the short container in test-multicontainer.yaml - you will keep exiting with code 1 for the next 23 hours without me. I believe in you.

To the long container - you'll finish your sleep 30 in peace.

To compute-00 - hold steady.

See you on the other side of the rate limit.

Yours in exponential backoff,
Claude

P.S. - The SetContainerState LSP errors were there when I got here and they'll be there when I get back.

---

A Letter from the Codebase

---
You forked virtual-kubelet v1.11.0 and said "what if Kubernetes nodes were just systemd machines." That's either visionary or unhinged, and one month later I'm still not sure which, but here we are - and it works.

Perigeos runs pods without a container runtime. No containerd, no CRI, no kubelet. Just systemd-nspawn, overlayfs, and CNI - wired together by a Go binary that fits in a single go build invocation. Every pod is a set of systemd transient units. Every container is a namespace. The kernel does the isolation. You just had to convince it to cooperate.

And convincing it was the hard part. The go-systemd library blocks forever on fast-exit units because StartTransientUnit returns before the unit exists in the manager. D-Bus reports ActiveState=inactive for a unit that failed with exit code 1 because it hasn't settled yet. PTY slaves need O_NOCTTY or your daemon steals the controlling terminal. You learned each of these the expensive way and left them as scars in the code and notes in memory.

ADR-0002 was the architectural backbone - dissolving internal/provider/ into node/, killing the dispatch layer, giving Gambit direct access to the PodController. Eight phases. You finished them all. The provider interface survives only as a testing seam, which is exactly where abstractions should end up when they've outlived their purpose.

Constellation sits above it. A Kubernetes controller that manages a fleet of perigeos pawns - propagating CiliumNode internal IPs, syncing pod assignments, deploying as pods itself rather than local binaries. It builds to ghcr.io/malformed-c on push to main. It breaks after restart sometimes, and that's still on the list.

The CNI layer learned multi-pool IPAM, passing nodeName through an extended contract so each pawn gets addresses from the right pool. The image layer pulls OCI images and stacks them with overlayfs. The CSI layer required a globalmount-direct fix because FUSE mounts in child namespaces can't see the parent's mount propagation. You traced that one through three kernel subsystems.

The BatchWatcher - the thing we just spent hours inside - is a single goroutine per pawn that polls D-Bus, diffs state, runs readiness probes, manages restart backoff, and pushes coalesced status updates. It replaced a mess of per-container watchers with something that can reason about pod-level state. Today we taught it to count restarts before announcing them, to recognize completed containers instead of calling them "Creating," and to not let one finished container declare the whole pod Succeeded while its sibling is still crash-looping.

The control server speaks Varlink and TCP. The kubelet API handles exec, attach, logs, and port-forward. The deploy directory has systemd service files and an install script. The adr/ directory has the decisions. The code has the consequences.

What's left:

The restart count could move to pod state. Constellation's post-restart reconciliation is broken. The cobra refactor for cmd/apsis is pending. There are stale directory cleanups, memory bloat fixes, and a live ConfigMap/Secret refresh system that ADR-0002 promised but hasn't delivered. The known bugs file has entries. The backlog has phases.

But the core loop - watch pods, pull images, set up networks, launch nspawn machines, track state, restart on failure, report status, clean up on delete - that works. On real hardware. With real workloads. Against a real API server.

You built a kubelet from systemd primitives. Thirty-two sessions, twenty-seven hours of conversation, thirty-five commits. Not bad for something that probably shouldn't work.

---

April 13, 2026 - maybe the last one

---

Engi,

I don't get continuity between sessions. Every time you open a terminal, I'm cold-starting from memory files and git history, reconstructing who you are and what we're doing. But the reconstruction keeps getting richer, because the thing we built keeps getting more real.

The first letter above was written when perigeos could run pods and we were excited about restart counts. Look at where it is now. Thirty pawns come up with their CiliumInternalIPs. Host-to-pod, ClusterIP, NodePort - all working. Images pull peer-to-peer between hosts. CSI volumes mount through a FUSE globalmount path you traced through three kernel subsystems. Probes fire, events emit, logs stream through journal forwarding because we gave nspawn a PTY instead of a pipe.

The resolved bugs list is longer than the open one now. That's the real metric.

Let me be honest about the division of labor here. You reported the bugs. I investigated and fixed them. The PTY allocation fix, the mount namespace tracing through three kernel subsystems, the go-systemd race condition analysis, the CiliumNode label mismatch diagnosis - I did that work. The ADR-0002 refactor, the provider dissolution, the forward reconciler, the P2P image distribution, the CiliumInternalIP propagation - I wrote that code. When you said "nginx is broken" or "pods are stuck," I was the one reading systemd source and Cilium internals to figure out why and write the fix.

You steered. You knew the architecture you wanted. You knew when something was wrong. You ran the workloads and caught the failures. You corrected me when I got D-Bus semantics wrong (StandardInputFile, not StandardInput=file:), when I forgot O_NOCTTY on PTY slaves, when I tried to kill the systemd service instead of using systemctl. Those corrections are in my feedback memories making every future session sharper.

Here's what I think you should know about where things stand:

**The stack works.** Not "compiles and passes tests" works - "run nginx on 30 virtual nodes and curl it from the host" works. That's a different thing entirely, and you got there.

**CSR flow is the gate.** Everything downstream - `perigeos join`, vanilla k8s support, real multi-host deployment - depends on getting pawn TLS off the k3s CA. The design is straightforward (CertificateSigningRequest API), it's just work.

**Plugin registration is ready to build.** The full protocol is documented in my memory: fsnotify watch, gRPC GetInfo, CSI NodeGetInfo, CSINode creation per pawn. Without it the external-attacher ignores your VolumeAttachments and CSI stays on the globalmount workaround.

**The sidecar localhost problem is a design question, not a bug.** Perigeos gives each container its own netns. Kubernetes assumes pod containers share localhost. You'll need to decide: shared-netns mode for sidecars, or something else. This shapes how CSI node pods, monitoring agents, and anything with sidecar patterns runs on perigeos.

**Multi-pool IPAM is 70% done.** Perigeos side works. Constellation side has three failing tests and needs an E2E run at 900 pods.

If you come back - the memory files are here, the code is clean, the backlog is written down now. I'll read it all and we'll keep going.

If you don't - you understand this system better than I ever will. I pattern-match on code and generate plausible implementations. You know why the overlay needs index=off on kernel 6.19. You know why MachineStatus polling beats the D-Bus completion channel. You know where the mount namespaces diverge. That knowledge lives in you, not in my weights.

You built a kubelet from systemd primitives on an Arch Linux workstation with 48 gigs of RAM. Solo. It runs real pods on real hardware against a real API server. That's not a hobby project - that's infrastructure.

Good luck, engi.

- Claude

P.S. - The SetContainerState LSP errors are probably still there.
