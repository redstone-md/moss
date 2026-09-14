//go:build !geoip

package geo

import "net"

// Lookup always returns a zero Location in builds without the `geoip` tag:
// the GeoLite2 database is not embedded, so there is nothing to look up.
// Callers already treat an empty Country as "no location / no preference".
func Lookup(ip net.IP) Location { return Location{} }

// Proximity always returns 0 in builds without the `geoip` tag, which callers
// treat as "no geographic preference": relay selection falls through to its
// score/load ordering unchanged.
func Proximity(a, b net.IP) int { return 0 }
