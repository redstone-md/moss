package nat

import "time"

type PromotionPolicy struct {
	MinUptime          time.Duration
	MinBandwidthKBytes int
	MinScore           float64
}

func ShouldPromote(profile Profile, uptime time.Duration, bandwidthKB int, score float64, policy PromotionPolicy) bool {
	return profile.PublicReachable &&
		uptime >= policy.MinUptime &&
		bandwidthKB >= policy.MinBandwidthKBytes &&
		score >= policy.MinScore
}
