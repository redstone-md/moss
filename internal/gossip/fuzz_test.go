package gossip

import (
	"encoding/json"
	"testing"
	"time"
)

func FuzzEnvelopeJSONRoundTrip(f *testing.F) {
	f.Add([]byte(`{"type":"publish","channel":"alpha","message_id":"1","payload":"aGVsbG8="}`))
	f.Add([]byte(`{"type":"ihave","channel":"beta","message_ids":["a","b"]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var env Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return
		}
		cache := NewCache(time.Minute)
		if env.MessageID != "" {
			cache.Store(env)
			_, _ = cache.Get(env.MessageID)
			_ = cache.Seen(env.MessageID)
		}
		_, _ = json.Marshal(env)
	})
}
