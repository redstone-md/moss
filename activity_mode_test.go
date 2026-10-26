package moss_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	moss "github.com/redstone-md/moss"
)

func TestGoHostCanOptIntoWaitingWithoutRecreatingNode(t *testing.T) {
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("d8:intervali1e5:peers0:e")) }))
	defer tracker.Close()
	off := false
	n, err := moss.NewNode("waiting-go-host", nil, moss.Config{Trackers: []string{tracker.URL}, DHTEnabled: &off, LANDiscoveryEnabled: &off})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	identity := n.PublicKey()
	if err = n.SetActivityMode(moss.ActivityMode(-1)); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if err = n.SetActivityMode(moss.ActivityWaiting); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var state struct {
			Mode         string `json:"activity_mode"`
			Reachability string `json:"waiting_reachability"`
		}
		if err = json.Unmarshal([]byte(n.MeshInfoJSON()), &state); err != nil {
			t.Fatal(err)
		}
		if state.Mode == "waiting" && state.Reachability == "unavailable" {
			if n.PublicKey() != identity {
				t.Fatal("node identity replaced")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("host request never converged")
}
