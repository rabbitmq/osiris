# Osiris TLA+ Specification

This directory contains a TLA+ model of the Osiris log replication protocol. The spec models message-passing between a coordinator and a set of replicas, covering leader election, fencing, log replication, and retention.

## Module Overview

- **OsirisMsgPassing.tla**: the main specification. Models replicas (leader/follower), a coordinator that manages epochs and elections, and the message-passing interactions between them.
- **MC.tla / MC.cfg**: model-checking configuration. Defines a model with 3 replicas (`r1, r2, r3`), 3 values (`a, b, c`), and state-space constraints (`coord_epoch < 4`, `start_stop_ctr < 4`).

## Invariants

The model checks the following invariants:

| Invariant | Description |
|-----------|-------------|
| `TypeOK` | Type correctness of all variables. |
| `NoDivergence` | A non-stale follower cannot hold a record that conflicts with the leader's log (same offset, different content). |
| `FollowerEqualOrLowerEpoch` | A follower's epoch never exceeds the leader's epoch. |
| `NoLossOfConfirmedWrite` | A confirmed write exists on at least one replica's log, unless every replica that held it has since retained past it. |
| `LerMatchesLog` | Each replica's Log End Record (LER) matches the highest offset in its log. |
| `FollowerCommittedOffsetBounded` | A follower's committed offset never exceeds the leader's committed offset. |
| `TestInv` | Placeholder for ad-hoc debugging expressions (always TRUE by default). |

## Running the Model

### Prerequisites

Install the [TLA+ tools](https://github.com/tlaplus/tlaplus). You can either:

- Use the TLA+ Toolbox IDE, or
- Use the command-line `tlc` model checker (available via `tla2tools.jar`)

### Using the TLA+ Toolbox

1. Open the Toolbox and create a new spec pointing to `OsirisMsgPassing.tla`.
2. Create a model using the constants and constraints defined in `MC.tla` / `MC.cfg`.
3. Run the model checker.

### Using the command line

Download [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) and run from this directory:

```bash
# Simulation mode (random exploration, good for finding deep bugs):
java -jar /path/to/tla2tools.jar -simulate -depth 500 -workers auto -deadlock MC.tla

# Exhaustive model checking:
java -jar /path/to/tla2tools.jar -workers auto -deadlock MC.tla
```

The `-deadlock` flag tells TLC not to treat deadlock as an error (the spec's state space is intentionally bounded).

The `states/` directory stores TLC state files from previous runs.
