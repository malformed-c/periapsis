---

### Implementation Plan: Pure Functional Core with `fogfish/golem`

#### Phase 1: Establish the Algebraic Data Types (ADTs)
We use `golem/pure` to create our inputs (`Facts`) and outputs (`Effects`) using Higher-Kinded Polymorphism.

1. **Define Facts (Input):**
   *(Already mostly done in your `fact.go`, just needs strict adherence).*
   ```go
   type FactKind any
   type Fact pure.HKT[FactKind, any] // HKT boundary
   ```
2. **Define Effects (Output):**
   Create a new file `types/effect.go`.
   ```go
   package types
   import "github.com/fogfish/golem/pure"
   
   type EffectKind any
   type Effect pure.HKT[EffectKind, any]
   
   type RestartContainer struct { UID, Container string }
   func (RestartContainer) HKT1(EffectKind) {}
   func (RestartContainer) HKT2(RestartContainer) {}
   
   type EmitStatus struct { Intent StatusIntent }
   func (EmitStatus) HKT1(EffectKind) {}
   func (EmitStatus) HKT2(EmitStatus) {}
   ```

#### Phase 2: Implement the Pure Reducer and Optics (`foci`)
Build the mathematically pure state transition function.

1. **Define Immutable State & Lenses:**
   ```go
   import "github.com/fogfish/golem/optics"

   type PodState struct {
       UID        string
       Phase      types.PodPhase
       Containers map[string]ContainerState
   }

   // golem/optics allows us to safely and immutably update the map
   var containersLens = optics.ForProduct3[PodState, map[string]ContainerState]()
   ```
2. **The Pure `Reduce` Function:**
   ```go
   func Reduce(state PodState, fact types.Fact) (PodState, []types.Effect) {
       var effects[]types.Effect
       
       switch f := fact.(type) {
       case types.UnitFact:
           // Using Optics to immutably update the container map without mutating the old state
           c := state.Containers[f.Container]
           if f.ExitCode != nil && *f.ExitCode != 0 {
               c.State = types.WaitingState("CrashLoopBackOff")
               
               nextState := optics.Put(containersLens, state, mapUpdate(state.Containers, f.Container, c))
               
               effects = append(effects, types.RestartContainer{UID: state.UID, Container: f.Container})
               effects = append(effects, types.EmitStatus{Intent: buildStatusIntent(nextState)})
               return nextState, effects
           }
       }
       return state, effects
   }
   ```

#### Phase 3: The Imperative Shell (`syzygy`)
`Syzygy` holds the runtime state and executes the effects. No locks are required since it runs sequentially.

1. **Delete `FocusRegistry`:** `Syzygy` now directly manages a `map[string]foci.PodState`.
2. **The Actor Loop:**
   ```go
   func (s *Syzygy) Run(ctx context.Context) {
       for fact := range s.inbox {
           uid := factUID(fact)
           currentState := s.states[uid] // fetches zero-value if new
           
           // PURE COMPUTATION
           nextState, effects := foci.Reduce(currentState, fact)
           s.states[uid] = nextState
           
           // EXECUTE EFFECTS
           for _, eff := range effects {
               switch e := eff.(type) {
               case types.RestartContainer:
                   go s.gambit.RestartContainerCB(ctx, e.UID, nil, e.Container, 0, 0)
               case types.EmitStatus:
                   s.horizon.WriteStatus(e.Intent)
               }
           }
       }
   }
   ```

#### Phase 4: Gut the God Objects (`BatchWatcher` & `PodStore`)
1. Remove all logic from `BatchWatcher` except the D-Bus subscription loop. It now just creates `types.UnitFact` and sends it to `Syzygy`.
2. Strip `PodStore` of `PodStatus`, restarts, and probe state fields. `PodStore` is now just a read-only cache of the Kubernetes desired `Spec`.

---

# ADR-0011: Functional Core, Imperative Shell using `fogfish/golem`

**Date:** 2026-04-15
**Status:** Accepted

## Context

Our node component is suffocating under the weight of Shared Mutable State. Three central components (`BatchWatcher`, `Gambit`, and `PodStore`) act as heavily intertwined "God Objects." 

Whenever a container exits or a probe fires, these components fight over `sync.RWMutex` locks inside `PodStore` to update restart backoffs, container phases, and readiness states. This produces:
1. **Race Conditions:** Split-brain writes to Kubernetes when multiple container events happen simultaneously.
2. **Deadlocks:** Heavy load (e.g., node startup with 3000+ pods) causes immense lock contention.
3. **Untestability:** State transitions are buried in imperative OS-level execution code, requiring massive dependency mocks to test basic pod state logic.

## Decision

We will adopt the **Functional Core, Imperative Shell** architectural pattern (often referred to as the Elm Architecture or Redux pattern), supercharged by the `github.com/fogfish/golem` library for functional programming in Go.

### 1. The Mathematical Core (`foci`)
We will replace the per-pod state machine actors with a single, mathematically pure function: `Reduce(PodState, Fact) -> (NewPodState,[]Effect)`.
* **Immutability via `golem/optics`:** State structs are strictly immutable. Deep updates to maps and nested structs will be handled safely using `golem` Lenses, ensuring no pointer contamination occurs.
* **Algebraic Data Types:** Inputs (`Facts`) and outputs (`Effects`) are modeled as Higher-Kinded Types (`pure.HKT`) to ensure the state machine is a closed, type-safe loop.

### 2. The Imperative Shell (`syzygy`)
`Syzygy` will cease to be a "router" and will become the sole State Manager Actor. 
* It runs a single-threaded loop holding a lock-free `map[string]foci.PodState`.
* It receives `Facts`, feeds them into the pure `foci.Reduce()` function, and persists the resulting `NewPodState`.
* It inspects the returned `
