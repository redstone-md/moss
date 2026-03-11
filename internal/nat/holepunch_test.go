package nat

import "testing"

func TestCoordinatorPlansPredictedPorts(t *testing.T) {
	plan := Coordinator{
		Attempts:           3,
		EnablePrediction:   true,
		LocalObservations:  []string{"198.51.100.10:40000", "198.51.100.10:40002", "198.51.100.10:40004"},
		RemoteObservations: []string{"203.0.113.20:50000", "203.0.113.20:50003", "203.0.113.20:50006"},
	}.Plan("198.51.100.10:40004", "203.0.113.20:50006")
	if len(plan) != 3 {
		t.Fatalf("unexpected plan length %d", len(plan))
	}
	if plan[0].Remote != "203.0.113.20:50006" || plan[1].Remote != "203.0.113.20:50009" || plan[2].Remote != "203.0.113.20:50012" {
		t.Fatalf("unexpected remote plan: %#v", plan)
	}
	if plan[0].Local != "198.51.100.10:40004" || plan[1].Local != "198.51.100.10:40006" || plan[2].Local != "198.51.100.10:40008" {
		t.Fatalf("unexpected local plan: %#v", plan)
	}
}

func TestCoordinatorFallsBackWithoutPrediction(t *testing.T) {
	plan := Coordinator{Attempts: 2}.Plan("198.51.100.10:40000", "203.0.113.20:50000")
	if len(plan) != 2 {
		t.Fatalf("unexpected plan length %d", len(plan))
	}
	for _, pair := range plan {
		if pair.Local != "198.51.100.10:40000" || pair.Remote != "203.0.113.20:50000" {
			t.Fatalf("unexpected fallback pair %#v", pair)
		}
	}
}
