package nat

import "testing"

// The step predictor must survive the port deltas real symmetric NATs produce:
// a handful of equal strides, then one outlier mapping. A majority vote over
// exact equality pins the first stride it saw; the median keeps its footing.
func TestPredictPortStepUsesMedianOfDiffs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		observations []string
		want         int
	}{
		{
			name:         "steady stride two",
			observations: []string{"203.0.113.10:41000", "203.0.113.10:41002", "203.0.113.10:41004"},
			want:         2,
		},
		{
			name:         "steady stride three",
			observations: []string{"203.0.113.10:50000", "203.0.113.10:50003", "203.0.113.10:50006"},
			want:         3,
		},
		{
			name:         "single outlier stride does not derail the median",
			observations: []string{"203.0.113.10:41000", "203.0.113.10:41002", "203.0.113.10:41009", "203.0.113.10:41011"},
			want:         2,
		},
		{
			name:         "one observation is not a stride",
			observations: []string{"203.0.113.10:41000"},
			want:         0,
		},
		{
			name:         "even diff count takes the lower middle",
			observations: []string{"203.0.113.10:40000", "203.0.113.10:40004", "203.0.113.10:40008", "203.0.113.10:40011", "203.0.113.10:40014"},
			want:         3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := predictPortStep(tc.observations); got != tc.want {
				t.Fatalf("predictPortStep(%v) = %d, want %d", tc.observations, got, tc.want)
			}
		})
	}
}
