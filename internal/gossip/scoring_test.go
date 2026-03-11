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

func TestApplyIPColocationPenaltyResetsWhenPeerBecomesUnique(t *testing.T) {
	engine := NewEngine()
	engine.Ensure("peer-1")
	engine.ApplyIPColocationPenalty("peer-1", 3)
	if score := engine.Score("peer-1"); score >= 0 {
		t.Fatalf("expected negative score with colocated peers, got %f", score)
	}
	engine.ApplyIPColocationPenalty("peer-1", 1)
	if score := engine.Score("peer-1"); score != 0 {
		t.Fatalf("expected colocation penalty to reset, got %f", score)
	}
}
