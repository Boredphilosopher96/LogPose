# ~~Phase 5: Distribution and Operations~~

**Done marker:** Phase 5 is complete.

## Goal

Grow LogPose from a strong single-node retrieval engine into a service platform with explicit runtime boundaries, operator-visible placement decisions, and deterministic system-level operational validation.

## Architectural Shift

Earlier phases focus on local correctness and planner quality.

This phase turns the single-node engine into something that is operable as a service platform:

- control-plane and data-plane responsibilities are split into explicit services
- collection lifecycle and placement decisions move onto the control plane
- runtime status and placement reasoning become operator-visible contracts
- the repository gains deterministic restart-aware simulation tests across service boundaries

The point is still not to distribute a prototype. The point is to make the service boundary explicit enough that later scale-out and remote-durability work land on a stable operational contract instead of a monolithic node abstraction.

## Delivered Changes

| Component | Delivered Change |
| --- | --- |
| Server runtime | `AppState` now owns explicit control-plane and data-plane services instead of routing everything through one undifferentiated service handle |
| Catalog and metadata | Collection placement is now a first-class operator-visible contract, with stable route kind plus a human-readable route reason surfaced per collection |
| API and CLI | REST, gRPC, the client crate, and the CLI now expose runtime status plus collection placement diagnostics under transport-parity contracts |
| Telemetry and operations | Runtime status now reports node role, engine identity, endpoint wiring, collection inventory, and aggregated maintenance backlog |
| Testing | LogPose now has deterministic control-plane/data-plane simulation scenarios for service-driven write, flush, WAL recovery, restart, recorded-placement, and wrong-plane rejection boundaries, plus REST/gRPC parity checks for runtime and placement contracts |

## What "Done" Looks Like

- runtime boundaries are explicit enough to operate and reason about
- control-plane and data-plane responsibilities stop leaking into each other
- placement, maintenance, and query execution are observable and diagnosable through operator-facing contracts
- operator tooling can answer where a collection is placed and why that placement route was selected
- restart-aware simulation tests validate the new operational boundary and visible data persistence instead of relying only on ad hoc integration checks

Phase 5 is done.

## Testing Direction

This phase is where the long-term testing target in [Testing](../testing.md) first becomes real:

- deterministic multi-component harnesses centered on the service boundary, with REST and gRPC read-side parity checks layered alongside them
- restart and recovery orchestration tests at the service boundary
- fixed placement and runtime-status scenarios with exact step traces, including recorded-route and wrong-plane rejection coverage
- simulation-style validation before looser chaos-style experimentation

The system should prefer deterministic simulation-style testing before reaching for looser chaos-style experiments.

## What This Unlocks

Once LogPose has trustworthy runtime boundaries and operational controls, the final phase can focus on broader scale-out hardening, remote-durability expansion, policy, and research-driven optimizations instead of still repairing basic service architecture.
