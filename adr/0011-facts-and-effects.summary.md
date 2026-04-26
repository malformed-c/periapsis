Refactoring Summary: BW -> Fully Event-Based
Core Change: BatchWatcher stripped from God Object -> Thin Adapter

Before (870 lines): BatchWatcher was a hybrid event+poll component that:

Ran a 2s ticker poll calling ListManagedMachines
Had its own checkPod(), runProbes(), maybeRestart() logic
Maintained state maps: prevStateMap, prevReady, seenRunning, restarting, stateCache
Ran a coalescer to push status updates via NotifyStatus
Handled restart policy decisions
Recorded k8s events directly

After (~150 lines): BatchWatcher is a pure D-Bus signal adapter that:

Subscribes to PropertiesChanged signals
Parses unit names -> emits UnitFact to Syzygy
That's it. No state, no probes, no restarts, no status pushes
Key Changes by File
File
Change
node/batchwatcher.go	Rewritten: removed poll/checkPod/runProbes/coalescer, only D-Bus -> UnitFact
node/batchwatcher_test.go	Rewritten for event-based architecture
internal/syzygy/syzygy.go	Added anti-entropy loop: ListManagedMachines -> ContainerStateFact emission for drifted containers
node/lifecycle.go	Wired facts: PodAdmitFact from CreatePod, MarkRunningFact from launchContainer, PodEvictFact from DeletePod; replaced BW.ContainerState() with direct Runtime.MachineStatus() calls
node/gambit.go	Added sendFact field; removed NotifyStatus/BuildPodStatusCB from BW deps; removed containerWatchPoll constant
node/status.go	Replaced BW.ContainerState cache with direct Runtime.MachineStatus() call
cmd/perigeos/main.go	Added runtimeLister adapter; wired MachineLister to Syzygy; wired ProbeScheduler

Architecture Flow (After)
text

D-Bus signal -> BW -> UnitFact -> Syzygy -> foci.Reduce -> Effects -> Horizon -> k8s API
                                  ↑
Anti-entropy (5s) -> ContainerStateFact ──┘

ProbeScheduler (2s) -> ProbeFact ──┘

CreatePod -> PodAdmitFact ──────────┘
launchContainer -> MarkRunningFact ─┘
DeletePod -> PodEvictFact ──────────┘
This completes Phase 4 of ADR-0011: "Gut the God Objects" - BatchWatcher is now just a thin D-Bus adapter, and all state management flows through the Syzygy event loop via the pure foci.Reduce function.

---

Two-Path Watching: Reactive + Reconciliatory
1. Syzygy's Anti-Entropy Loop - the consistency backbone (replaces the old BW poll)

text

every 5s -> ListManagedMachines() -> diff against state machines -> emit ContainerStateFacts -> inbox -> Reduce -> Effects
This is the slow path that catches anything the fast path missed. It's essentially what the old BW's 2s ticker did, but:

It diffs against Syzygy's state machines (single source of truth), not ad-hoc maps
It self-sends through the inbox so facts are processed in order with everything else
It skips ticks when the inbox is backlogged (doesn't pile on)
Interval went from 2s -> 5s because D-Bus signals handle the fast path now
2. BatchWatcher's D-Bus Signal Subscription - the reactive fast path

text

PropertiesChanged signal -> parse UnitFact -> SendFact -> Syzygy inbox -> Reduce -> Effects
This is the new reactive path. Container exits/starts are detected immediately via D-Bus signals - no polling delay at all.

The Split
What
Old BW
New Architecture
Fast path (container exit/start)	D-Bus signal + 2s ticker	D-Bus signal -> UnitFact -> Syzygy
Slow path (consistency catch-up)	Same 2s ticker (dual purpose)	Syzygy anti-entropy (5s)
State diffing	BW's prevStateMap / prevReady	Syzygy's foci.PodState state machines
Restart decisions	BW's seenRunning + restarting maps	foci.Reduce (pure function)
Status coalescing	BW push on change	Syzygy -> Reduce -> Effects -> Horizon

So the answer is: Syzygy watches managed machines via its anti-entropy loop, with BatchWatcher serving as just the D-Bus signal translator that feeds Syzygy real-time events. The old BW's poll-and-diff-everything role has been split into reactive signal forwarding (BW) and periodic reconciliation (Syzygy), both converging into the same inbox -> Reduce -> Effects pipeline.

---

ProbeScheduler - Still Tick-Based, But Decoupled
ProbeScheduler is intentionally still a polling loop and that's correct - it has to be. HTTP/TCP/exec probes are inherently active checks (you have to go poke the container), not passive events you can subscribe to. You can't get a D-Bus signal that says "the liveness probe would fail right now."

How it works now
text

every 2s -> Snapshot() all running pods -> for each container:
  ├── startup probe?  -> RunProbe -> ProbeFact -> sz.Send -> Syzygy inbox
  ├── liveness probe? -> RunProbe -> ProbeFact -> sz.Send -> Syzygy inbox
  └── readiness probe?-> RunProbe -> ProbeFact -> sz.Send -> Syzygy inbox
Concurrency bounded by sem channel (50 concurrent probes max).

What changed vs old BW
Old BW
New ProbeScheduler
Where probes ran	Inline in BW's poll cycle	Dedicated goroutine
Probe result handling	BW directly restarted containers, pushed status	Emits ProbeFact -> Syzygy -> foci.Reduce decides
Readiness tracking	BW's prevReady map + direct status push	ProbeFact.Ready -> Reduce -> recomputePhase -> Effect
Liveness restart	BW called RestartContainer directly	ProbeScheduler resets probe state; foci.Reduce sees the state change and emits restart Effect
Coupling	Probes tangled with poll/diff/coalesce/restart logic	Pure: just runs probes, emits facts

The key architectural point
ProbeScheduler is not a "watcher" in the same sense as BW or Syzygy's anti-entropy. It's an active prober - it can't be event-driven because probes are the event source. The event-based architecture doesn't eliminate the need to poll containers for health; it eliminates the need for the watcher to own that responsibility.

The old BW was doing three fundamentally different things in one loop:

Watching (D-Bus signals + poll diff) -> now BatchWatcher + Syzygy anti-entropy
Probing (HTTP/TCP/exec checks) -> now ProbeScheduler
Deciding (restart policy, status coalescing) -> now foci.Reduce (pure function)
ProbeScheduler is #2 extracted cleanly. It talks to Syzygy through the same Fact interface as everything else, so foci.Reduce still gets the final say on what happens.

---

After: PodStore -> foci
text

                    ┌─────────────────────────────────────┐
                    │          foci.PodState               │
                    │  (single source of truth)            │
                    │                                      │
                    │  • Container phase, Ready, ExitCode  │
                    │  • RestartCount, Backoff, LastStarted│
                    │  • Probe fail/success counts         │
                    │  • LastProbeTime, ProbeStartedAt     │
                    │  • PodSpec, PodIP, Phase             │
                    └──────────┬──────────────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
     ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
     │ BatchWatcher│   │ProbeScheduler│  │ lifecycle.go│
     │             │   │              │  │             │
     │ D-Bus signal│   │ Reads foci   │  │ Emits Facts │
     │ -> UnitFact  │   │ Runs probes  │  │ (Register,  │
     │ -> SendFact  │   │ -> ProbeFact  │  │  Promote,   │
     └─────────────┘   └──────────────┘  │  Delete)    │
                                         └─────────────┘
            │                  │                  │
            └──────────────────┼──────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │      Syzygy         │
                    │  inbox -> Reduce ->   │
                    │  Effects -> dispatch │
                    └──────┬──────────────┘
                           │
              ┌────────────┼────────────────┐
              ▼            ▼                ▼
     ┌────────────┐ ┌─────────────┐ ┌──────────────┐
     │  PodStore   │ │   Horizon   │ │  disk persist│
     │ (projection)│ │  (k8s API)  │ │              │
     │             │ │             │ │              │
     │ *corev1.Pod │ │ UpdateStatus│ │ state files  │
     │ nameIndex   │ │ RestartCont │ │              │
     │ resources   │ │ RecordEvent │ │              │
     │ hydrated    │ │ ResetUnit   │ │              │
     │ inFlight    │ │             │ │              │
     │ deleting    │ │             │ │              │
     └─────────────┘ └─────────────┘ └──────────────┘
PodStore is now a projection - it only gets written to through Syzygy Effects. All decision-relevant state lives in foci.PodState and is managed by the pure Reduce function. The closed loop is:

ProbeScheduler reads from foci.ContainerState (phase, probe counts, last-probe-time)
Runs probes (HTTP/TCP/exec)
Emits ProbeFact with updated counts
foci.Reduce updates ContainerState with new counts
Repeat on next tick
