# AGENTS.md

Guidance for AI agents that write or review code in this repository.

## What this repository is

`medios-tokens` is an experimental reference resource provider for
[Medios](https://github.com/medios-lab/medios). It brokers a consumable budget
(`reserve -> spend -> settle`) through the Medios resource provider HTTP API,
with no change to Medios core and no C++ coupling. Its value is as a small
program a reader can follow end to end and run as a demonstration.

## Build and test

```sh
go build ./...
go test -race -count=1 ./...
go vet ./...
```

The module uses the Go standard library only. A pull request that adds a module
dependency states the reason in its description.

## Code Review Rules

### Ledger accounting

- A task is debited once per `task_id`. Flag any path where a repeated or
  replayed Medios event (`TASK_ADDED`, `PUBLISH_RESOURCES`) changes the balance
  a second time.
- The in-memory ledger and its checkpoint file change together. Flag any path
  where the balance, the debit record or the provider ID changes in memory and
  the checkpoint write can then fail and leave the two different.
- A debit that Medios accepted is never dropped silently. Flag any path where a
  lost or restarted controller event stream leaves an accepted task undebited
  with no error and no retry.

### Advertised capacity

- The capacity sent to the node in `UPDATE_STATE` matches the ledger balance at
  the time it is sent. Flag concurrent advertisements that can arrive out of
  order and leave the node holding an older balance.
- Every balance change (debit, settle, refill) is followed by an advertisement.

### Scope

- Flag a change that needs a modification to Medios core, or that imports
  Medios internals, to work.
- Flag abstraction, configuration or infrastructure that the demonstration in
  the pull request does not use.
- High availability, production authentication and operational features are
  outside this repository's scope. Raise them only when the experiment the pull
  request describes depends on one.

### Reporting

- Report a defect with the event sequence or input that produces it.
- Label a suggestion that has no demonstrated failure as hardening, separately
  from defects.
