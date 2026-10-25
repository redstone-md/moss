# Operation-local interface inventory

The Mosh issue-94 Windows profile identifies repeated adapter enumeration in
MeshInfoJSON, LAN beacon construction and maintenance address selection. In
Go 1.25.9 Windows, both net.Interfaces and each Interface.Addrs call enumerate
all adapters. The authorized change batches those reads within one operation.

Scope: private interface inventory, advertised-address selection, LAN beacon
target construction. Keep the existing C/Go API, wire formats, selection and
VPN/virtual-interface policy, binding, discovery cadence and retry policy. No
cache survives an operation. Existing golang.org/x/sys/windows supplies the
Windows API; other platforms retain standard-library enumeration.

Checks: first reproduce multiple inventory/address reads through actual beacon
preparation; then require one fresh read per operation. Compare ordered native
Windows inventories with net.Interfaces/Interface.Addrs, cover IPv4/IPv6,
anycast, indices, flags, interface errors and native buffer growth. Preserve
address selection and broadcast targets. Run full tests, race/vet, Windows
checks, shared-library builds, and same-host alternating release measurements
with output goldens before updating the Mosh pin. Consumer compilation is a
separate compatibility check. Owner whole-application acceptance stays open.

The existing node_advertise.go and lan_discovery.go files exceed the Mosh
400-line limit before this change. Keep policy functions there; new inventory
and beacon-data construction live in small private files. No policy refactor
is included.
