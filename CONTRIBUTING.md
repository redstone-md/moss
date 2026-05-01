# Contributing

## Before you start

- use `dev` for active work
- keep changes focused and atomic
- prefer fixing root causes over adding special cases

## Development workflow

1. Create a branch from `dev`.
2. Make a narrowly scoped change.
3. Add or update tests.
4. Run the relevant local verification commands.
5. Open a pull request against `dev`.

## Required local checks

At minimum:

```bash
go test ./... -count=1 -timeout 1800s
```

If you touch FFI behavior, also rebuild the shared library:

```bash
go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi
```

On Unix-like systems:

```bash
go build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi
```

## Commit style

Use Conventional Commits:

- `feat:`
- `fix:`
- `docs:`
- `ci:`
- `test:`
- `chore:`

## Pull request expectations

A good pull request includes:

- what changed
- why it changed
- how it was verified
- any behavior or compatibility risks

## Design expectations

- keep runtime code readable and testable
- avoid unnecessary coupling across `internal/*` packages
- preserve FFI compatibility unless the change is intentional and documented
- validate all new external inputs

## Separate client repository

Desktop chat clients live in [MOSH](https://github.com/redstone-md/mosh).

Changes to the shared runtime contract should update:

- `docs/API.md`
- `docs/SHARED_INTEGRATION.md`
- MOSH integration code if the desktop client is affected
