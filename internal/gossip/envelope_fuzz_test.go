package gossip

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// FuzzEnvelopeUnmarshalAndCache drives the envelope JSON decoder with
// malformed, hostile, and oversize input, then exercises the seen-cache with
// whatever survived decoding. This is the front door of every inbound mesh
// frame, so the pinned contracts are load-bearing:
//
//   - json.Unmarshal never panics on any input; it either errors cleanly or
//     yields an Envelope.
//   - a decoded envelope is wire-stable: Marshal → Unmarshal → Marshal is
//     byte-identical (relays re-serialize envelopes, so the second encode
//     must not drift).
//   - the cache admits any decoded envelope: StoreIfNew accepts a fresh id
//     exactly once, Store keeps the payload replayable via Get, Seen
//     observes it, and RecentIDs surfaces a just-stored envelope as its
//     channel's newest id.
//
// A fresh Cache per iteration bounds what one input can pin in memory; the
// cache's own flood caps are the live node's defense, not a single input's.
func FuzzEnvelopeUnmarshalAndCache(f *testing.F) {
	f.Add([]byte(`{"type":"publish","channel":"alpha","message_id":"1","payload":"aGVsbG8="}`))
	f.Add([]byte(`{"type":"ihave","channel":"beta","message_ids":["a","b"]}`))
	f.Add([]byte(`{"type":"peer_announce","advertised_peer_id":"p","advertised_addr":"127.0.0.1:1","advertised_noise_static":"AQID","advertised_signature":"AAAA"}`))
	f.Add([]byte(`{"type":"ov_nodes","ov_contacts":[{"id":"AAAA","addr":"127.0.0.1:2"}],"ov_providers":[{"peer":"AgQD"}]}`))
	f.Add([]byte(`{"type":"direct","sender_id":"!!!not-base64!!!"}`))
	f.Add([]byte(`{"type":"publish","payload":"aGVsbG8"}`))          // invalid base64 length
	f.Add([]byte(`{"type":"publish","payload":"!!!!"}`))             // invalid base64 alphabet
	f.Add([]byte(`{"type":"ping","sequence":18446744073709551616}`)) // uint64 overflow
	f.Add([]byte(`{"type":"ping","sequence":-1}`))
	f.Add([]byte(`{"type":"hole_punch_coord","coord_stage":"x","coord_at":-9223372036854775809}`))
	f.Add([]byte(`{"type":"graft"`))   // truncated
	f.Add([]byte(`{"type":"graft","`)) // truncated mid-key
	f.Add([]byte(`{`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`null`))
	f.Add([]byte(`""`))
	f.Add([]byte(`{"channel":"a","channel":"b","message_id":"dup"}`)) // duplicate keys
	f.Add([]byte(`{"type":"room_invite","signature":"AAAA"}`))
	f.Add([]byte(`{"message_ids":["` + strings.Repeat("x", 4096) + `"]}`)) // oversize id
	f.Add([]byte(`{"type":"iwant","message_ids":[` + strings.Repeat(`"id",`, 512) + `"id"]}`))
	f.Add([]byte(`{"type":"pong","sender_id":"AA==","relay_signature":"AAAA","reachable":true}`))
	f.Add([]byte{0x00, 0x01, 0x02, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		var env Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return // clean error, never a panic
		}

		// Wire stability: whatever the decoder produced must survive a
		// marshal/unmarshal/marshal cycle byte-identically.
		encoded, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("Marshal(decoded envelope) failed on input %q: %v", data, err)
		}
		var re Envelope
		if err := json.Unmarshal(encoded, &re); err != nil {
			t.Fatalf("Unmarshal(own Marshal output) failed: %v", err)
		}
		reencoded, err := json.Marshal(re)
		if err != nil {
			t.Fatalf("second Marshal failed: %v", err)
		}
		if string(encoded) != string(reencoded) {
			t.Fatalf("envelope marshal is not wire-stable:\nfirst:  %s\nsecond: %s", encoded, reencoded)
		}

		// Dedup admission: a fresh id is admitted exactly once.
		cache := NewCache(time.Minute)
		if !cache.StoreIfNew(env) {
			t.Fatalf("StoreIfNew rejected a fresh envelope: %+v", env)
		}
		if cache.StoreIfNew(env) {
			t.Fatalf("StoreIfNew admitted the same envelope twice: %+v", env)
		}

		// Replay: Store keeps the payload retrievable, and the ring
		// surfaces it as the channel's newest id.
		cache.Store(env)
		if env.MessageID != "" {
			got, ok := cache.Get(env.MessageID)
			if !ok {
				t.Fatalf("Get missed the envelope Store just kept: %+v", env)
			}
			if gotJSON, err := json.Marshal(got); err != nil {
				t.Fatalf("Marshal(cache Get result) failed: %v", err)
			} else if string(gotJSON) != string(encoded) {
				t.Fatalf("Get returned a different envelope: %s != %s", gotJSON, encoded)
			}
			if !cache.Seen(env.MessageID) {
				t.Fatalf("Seen missed a live envelope: %+v", env)
			}
		}
		if env.Channel != "" && env.MessageID != "" {
			ids := cache.RecentIDs(env.Channel, 64)
			if len(ids) == 0 || ids[0] != env.MessageID {
				t.Fatalf("RecentIDs(%q) = %v; want newest %q", env.Channel, ids, env.MessageID)
			}
		}
	})
}
