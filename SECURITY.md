# Security Policy

## Supported branches

Security fixes are applied on a best-effort basis to:

- `main`
- `dev`

Tagged releases are preferred for downstream consumption. If a security fix lands on `dev` first, it may be promoted to `main` after verification.

## Reporting a vulnerability

Do not open public GitHub issues for suspected vulnerabilities.

Send a private report to the repository maintainers with:

- affected version, commit, or artifact
- impact summary
- reproduction steps or proof of concept
- any suggested mitigation

If direct maintainer contact is unavailable, open a GitHub issue with no exploit details and state that you need a private security contact.

## Scope

Security-sensitive areas in this repository include:

- `cmd/moss-ffi` shared-library boundary
- `internal/transport` encrypted session handling
- `internal/bootstrap` tracker parsing and announce handling
- `internal/nat` NAT traversal and relay behavior

Out-of-scope items usually include:

- local development environment issues
- unsupported forks
- social engineering or physical access scenarios

## Disclosure expectations

- reasonable time for triage is expected before public disclosure
- coordinated disclosure is preferred
- fixes may land together with tests and hardening notes
