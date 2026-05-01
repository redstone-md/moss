package gossip

import "testing"

func TestScoringEngineTracksRewardsAndPenalties(t *testing.T) {
	engine := NewEngine()
	engine.Ensure("peer-1")
	engine.RewardFirstDelivery("peer-1")
	if score := engine.Score("peer-1"); score <= 0 {
		t.Fatalf("expected positive score after reward, got %f", score)
	}
	engine.PenalizeInvalid("peer-1")
	if score := engine.Score("peer-1"); score >= 0 {
		t.Fatalf("expected negative score after invalid penalty, got %f", score)
	}
}

func TestPenalizeMeshDeliveryDeficit(t *testing.T) {
	engine := NewEngine()
	engine.Ensure("peer-1")
	engine.PenalizeMeshDelivery("peer-1")
	if score := engine.Score("peer-1"); score != -0.5 {
		t.Fatalf("expected mesh delivery deficit penalty, got %f", score)
	}
}

func TestApplyIPColocationPenaltyResetsWhenPeerBecomesUnique(t *testing.T) {
	engine := NewEngine()
	engine.Ensure("peer-1")
	engine.ApplyIPColocationPenalty("peer-1", 4)
	if score := engine.Score("peer-1"); score >= 0 {
		t.Fatalf("expected negative score with colocated peers, got %f", score)
	}
	engine.ApplyIPColocationPenalty("peer-1", 1)
	if score := engine.Score("peer-1"); score != 0 {
		t.Fatalf("expected colocation penalty to reset, got %f", score)
	}
}

func TestApplyIPColocationPenaltyPenalizesEveryAdditionalPeerPerIP(t *testing.T) {
	engine := NewEngine()
	engine.Ensure("peer-1")
	engine.ApplyIPColocationPenalty("peer-1", 2)
	if score := engine.Score("peer-1"); score != -5 {
		t.Fatalf("expected penalty -5 for two peers behind one public IP, got %f", score)
	}

	engine.ApplyIPColocationPenalty("peer-1", 4)
	if score := engine.Score("peer-1"); score != -15 {
		t.Fatalf("expected penalty -15 for four peers behind one public IP, got %f", score)
	}
}
