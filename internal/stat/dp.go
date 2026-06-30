package stat

import (
	"math"
	"math/rand"
)

// clampU64 bounds v to [0, cap]. Clamping a per-node contribution before it is
// reported bounds the sensitivity of the aggregate sum, which is what makes the
// differential-privacy guarantee meaningful: one node can move the published
// total by at most `cap`.
func clampU64(v, cap uint64) uint64 {
	if cap == 0 {
		return 0
	}
	if v > cap {
		return cap
	}
	return v
}

// laplaceNoise samples from a Laplace(0, scale) distribution using the
// inverse-CDF method. scale = sensitivity/epsilon; a smaller epsilon means more
// noise and stronger privacy. The randomness source need not be cryptographic —
// the secret being protected is the node's true value, and the noise only has to
// be unpredictable to an observer, which a per-node math/rand stream satisfies.
func laplaceNoise(rng *rand.Rand, scale float64) float64 {
	if scale <= 0 {
		return 0
	}
	// u in (-0.5, 0.5]; sign(u)*ln(1-2|u|) gives a Laplace sample.
	u := rng.Float64() - 0.5
	if u <= -0.5 {
		u = -0.499999999
	}
	return -scale * math.Copysign(math.Log(1-2*math.Abs(u)), u)
}

// noiseAndClamp clamps value to [0, cap], adds Laplace noise calibrated to
// (cap, epsilon), and floors the result at 0. The clamped+noised value is what a
// node gossips for its own per-epoch entry; receivers never re-noise, so the
// CRDT stays convergent.
func noiseAndClamp(rng *rand.Rand, value, cap uint64, epsilon float64) uint64 {
	clamped := clampU64(value, cap)
	if epsilon <= 0 || cap == 0 {
		return clamped
	}
	noisy := float64(clamped) + laplaceNoise(rng, float64(cap)/epsilon)
	if noisy < 0 {
		return 0
	}
	return uint64(noisy + 0.5)
}
