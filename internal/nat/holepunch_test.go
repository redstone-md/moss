package nat

import "testing"

func TestCoordinatorPlansPredictedPorts(t *testing.T) {
	plan := Coordinator{
		Attempts:           3,
		EnablePrediction:   true,
		LocalObservations:  []string{"8.8.8.8:40000", "8.8.8.8:40002", "8.8.8.8:40004"},
		RemoteObservations: []string{"1.1.1.1:50000", "1.1.1.1:50003", "1.1.1.1:50006"},
	}.Plan("8.8.8.8:40004", "1.1.1.1:50006")
	if len(plan) != 3 {
		t.Fatalf("unexpected plan length %d", len(plan))
	}
	if plan[0].Remote != "1.1.1.1:50006" || plan[1].Remote != "1.1.1.1:50009" || plan[2].Remote != "1.1.1.1:50012" {
		t.Fatalf("unexpected remote plan: %#v", plan)
	}
	if plan[0].Local != "8.8.8.8:40004" || plan[1].Local != "8.8.8.8:40006" || plan[2].Local != "8.8.8.8:40008" {
		t.Fatalf("unexpected local plan: %#v", plan)
	}
}

func TestCoordinatorDoesNotPredictPrivateRemotePorts(t *testing.T) {
	plan := Coordinator{
		Attempts:           3,
		EnablePrediction:   true,
		RemoteObservations: []string{"127.0.0.1:20000", "127.0.0.1:20001", "127.0.0.1:20002"},
	}.Plan("8.8.8.8:40000", "127.0.0.1:20000")
	for _, pair := range plan {
		if pair.Remote != "127.0.0.1:20000" {
			t.Fatalf("unexpected private predicted remote: %#v", plan)
		}
	}
}

func TestCoordinatorDoesNotPredictAcrossRemoteHosts(t *testing.T) {
	plan := Coordinator{
		Attempts:           3,
		EnablePrediction:   true,
		RemoteObservations: []string{"1.1.1.1:20000", "1.1.1.1:20001", "1.1.1.1:20002"},
	}.Plan("8.8.8.8:40000", "8.8.4.4:20000")
	for _, pair := range plan {
		if pair.Remote != "8.8.4.4:20000" {
			t.Fatalf("unexpected cross-host predicted remote: %#v", plan)
		}
	}
}

func TestCoordinatorSkipsOutOfRangePredictedPorts(t *testing.T) {
	plan := Coordinator{
		Attempts:           3,
		EnablePrediction:   true,
		RemoteObservations: []string{"1.1.1.1:65532", "1.1.1.1:65534"},
	}.Plan("8.8.8.8:40000", "1.1.1.1:65534")
	for _, pair := range plan {
		if pair.Remote != "1.1.1.1:65534" {
			t.Fatalf("unexpected out-of-range predicted remote: %#v", plan)
		}
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
