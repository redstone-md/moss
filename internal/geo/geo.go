// Package geo maps peer IP addresses to a coarse location (country /
// continent) so relay selection can prefer a relay close to the peer it must
// reach, shortening the relay↔target leg.
//
// The GeoLite2-Country database is embedded (gzip-compressed) only in builds
// tagged `geoip`. The tag is OFF by default: the 4.3 MB database would cost
// ~27% of the binary plus ~8 MB of RAM on the first lookup, so default builds
// ship without it. Without the tag every Lookup returns a zero Location and
// Proximity returns 0, so callers must treat "no location" as "no
// preference" — relay selection then orders candidates by score and load
// alone, exactly as it already does for private/unknown IPs.
package geo

// Location is a coarse geographic position: an ISO country code (e.g. "US")
// and a continent code (e.g. "NA"). Empty fields mean the IP was not found —
// because it is private/unknown, the database failed to load, or the binary
// was built without the `geoip` tag.
type Location struct {
	Country   string
	Continent string
}
