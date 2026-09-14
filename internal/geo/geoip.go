//go:build geoip

package geo

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"io"
	"net"
	"sync"

	"github.com/oschwald/maxminddb-golang"
)

//go:embed GeoLite2-Country.mmdb.gz
var mmdbGz []byte

var (
	once   sync.Once
	reader *maxminddb.Reader
)

func load() {
	gz, err := gzip.NewReader(bytes.NewReader(mmdbGz))
	if err != nil {
		return
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		return
	}
	if r, err := maxminddb.FromBytes(data); err == nil {
		reader = r
	}
}

// Lookup returns the coarse location of an IP, or a zero Location when the
// database has no entry (private/unknown IPs, or if the DB failed to load).
func Lookup(ip net.IP) Location {
	once.Do(load)
	if reader == nil || ip == nil {
		return Location{}
	}
	var rec struct {
		Country struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
		Continent struct {
			Code string `maxminddb:"code"`
		} `maxminddb:"continent"`
	}
	if err := reader.Lookup(ip, &rec); err != nil {
		return Location{}
	}
	return Location{Country: rec.Country.ISOCode, Continent: rec.Continent.Code}
}

// Proximity scores how geographically close two IPs are: 2 = same country,
// 1 = same continent, 0 = different or unknown. Higher is closer. Used as a
// relay-selection preference, so an unknown IP simply carries no preference.
func Proximity(a, b net.IP) int {
	la := Lookup(a)
	if la.Country == "" {
		return 0
	}
	lb := Lookup(b)
	switch {
	case la.Country == lb.Country:
		return 2
	case la.Continent != "" && la.Continent == lb.Continent:
		return 1
	default:
		return 0
	}
}
