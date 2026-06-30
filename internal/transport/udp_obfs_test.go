package transport

import (
	"context"
	"strconv"
	"testing"
	"time"

	mcrypto "moss/internal/crypto"
)

func TestUDPListenersConnectThroughCodec(t *testing.T) {
	srvIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("server identity failed: %v", err)
	}
	cliIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("client identity failed: %v", err)
	}
	makeCfg := func(identity *mcrypto.Identity) HandshakeConfig {
		return HandshakeConfig{
			MeshID:      "obfs-mesh",
			PSK:         []byte("01234567890123456789012345678901"),
			Identity:    identity,
			ObfsPadMax:  64,
			ObfsPadData: true,
		}
	}
	srv, srvPort, err := ListenUDP(0, makeCfg(srvIdentity))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, _, err := ListenUDP(0, makeCfg(cliIdentity))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	srvDone := make(chan error, 1)
	go func() {
		_, err := srv.Accept()
		srvDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := cli.DialContext(ctx, "127.0.0.1:"+strconv.Itoa(srvPort)); err != nil {
		t.Fatalf("dial through codec failed: %v", err)
	}
	select {
	case err := <-srvDone:
		if err != nil {
			t.Fatalf("server accept failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server accept timed out")
	}
}
