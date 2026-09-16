// Command moss-lan is the LAN-shaped front door to moss: create or join a
// private room, get a virtual IP inside it, and discover the other
// participants by nickname. By default the intranet edge is an injected
// tun.Loopback, so packets are routable in-process only — enough to exercise
// room membership, presence and invites without a kernel device. Pass --tun to
// open a real kernel TUN interface instead (see internal/meshlan.OpenTun), so
// the OS routes IP traffic over the mesh.
//
// The two subcommands:
//
//	moss-lan create --nick alice --room vault --psk s3cret --cidr 10.66.0.0/24
//	    → starts a node in a fresh private room, prints its own peer ID,
//	      virtual IP, and one moss-lan:// join string per --invitee (or
//	      itself, ready to hand out), then runs until SIGINT/SIGTERM.
//	moss-lan join --nick bob --invite moss-lan://… [--cidr 10.66.0.0/24]
//	    → unpacks the invite, joins the room on the underlying node, prints
//	      its virtual IP, then runs until SIGINT/SIGTERM.
//
// Invite strings are QR-encodable text: hand `moss-lan create`'s output to
// any external QR encoder (e.g. `qrencode -t UTF8`) and scan it on the
// joining side.
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/meshlan"
	"github.com/redstone-md/moss/internal/tun"
)

// version is stamped at build time (-ldflags "-X main.version=...").
var version = "dev"

const defaultCIDR = "10.66.0.0/24"

func main() {
	log.SetFlags(0)
	log.SetPrefix("moss-lan: ")

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "create":
		runCreate(os.Args[2:])
	case "join":
		runJoin(os.Args[2:])
	case "version":
		fmt.Printf("moss-lan %s\n", version)
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `moss-lan — приватная LAN поверх moss

  moss-lan create --nick <ник> --room <id> --psk <psk> [--cidr <cidr>] [--invitee <peerID>]… [--tun [--tun-name <name>]]
        создать комнату; печатает свой peer ID, виртуальный IP и
        moss-lan:// инвайты (по одному на --invitee)

  moss-lan join --nick <ник> --invite <moss-lan://…> [--cidr <cidr>] [--tun [--tun-name <name>]]
        войти по инвайту; печатает свой виртуальный IP

  Флаги:
    --nick     ник, 1–32 символа [a-zA-Z0-9_-]
    --room     id комнаты (create)
    --psk      пароль комнаты (create)
    --invitee  peer ID приглашаемого; повторяется (create)
    --invite   инвайт-строка moss-lan://… (join)
    --cidr     пул виртуальных IP (по умолчанию `+defaultCIDR+`)
    --tun      открыть реальный TUN-интерфейс вместо loopback;
               требует CAP_NET_ADMIN/root (Linux/macOS) или
               wintun.dll рядом с бинарём (Windows, см. MOSS_WINTUN_DLL)
    --tun-name имя интерфейса ("tun0", "utun5"); пусто = авто

  QR: инвайт — обычная строка. Любой внешний QR-энкодер
  (например `+"`"+`qrencode -t UTF8 <строка>`+"`"+`) закодирует её как есть.
`)
}

// isolatedConfig builds a mesh config with no public discovery: no
// trackers, no DHT, no LAN beacons. A moss-lan room is invitation/PSK
// territory; the public substrate is not where it looks for peers.
func isolatedConfig(name string) mesh.Config {
	cfg := mesh.DefaultConfig()
	cfg.NetworkID = "moss-lan-" + name
	cfg.Trackers = nil
	cfg.DHTEnabled = false
	cfg.LANDiscoveryEnabled = false
	cfg.GossipSub.HeartbeatMS = 100
	return cfg
}

// openIface builds the intranet edge for a run: the real kernel TUN device
// when --tun was passed, the in-process loopback otherwise. It is the seam the
// MVP header points at — NewLanNode/AttachTun take any tun.PacketIface, so
// toggling the device here is all it takes to make the OS route IP over the
// mesh. A TUN that cannot be opened is fatal: silently falling back to the
// loopback would print a virtual IP that ping can never reach, the worst kind
// of "it worked" lie.
func openIface(useTun bool, name string) tun.PacketIface {
	if !useTun {
		return tun.NewLoopback()
	}
	iface, err := meshlan.OpenTun(name)
	if err != nil {
		if errors.Is(err, meshlan.ErrNotImplemented) {
			log.Fatalf("--tun: %v (эта ОС пока без драйвера TUN; убери --tun для loopback)", err)
		}
		log.Fatalf("--tun: не удалось открыть интерфейс: %v (нужны CAP_NET_ADMIN/root; на Windows — wintun.dll рядом с бинарём)", err)
	}
	if named, ok := iface.(meshlan.TunIface); ok {
		log.Printf("tun: открыт реальный интерфейс %q", named.Name())
	}
	return iface
}

// runCreate: build the room, print identity + invites, run until signal.
func runCreate(args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	nick := fs.String("nick", "", "ник (1–32 символа [a-zA-Z0-9_-])")
	room := fs.String("room", "", "id комнаты")
	psk := fs.String("psk", "", "пароль комнаты")
	cidr := fs.String("cidr", defaultCIDR, "пул виртуальных IP")
	useTun := fs.Bool("tun", false, "реальный TUN-интерфейс вместо loopback")
	tunName := fs.String("tun-name", "", "имя TUN-интерфейса (пусто = авто)")
	invitees := multiFlag{}
	fs.Var(&invitees, "invitee", "peer ID приглашаемого (повторяется)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("create: %v", err)
	}
	if *nick == "" || *room == "" || *psk == "" {
		log.Fatal("create: --nick, --room и --psk обязательны")
	}
	if err := meshlan.ValidateNick(*nick); err != nil {
		log.Fatalf("create: %v", err)
	}

	// The node lives in the room it is born into (meshID = --room, psk).
	// moss-lan owns this node's full lifecycle: start, run, stop.
	node, err := mesh.NewNode(*room, []byte(*psk), isolatedConfig(*room))
	if err != nil {
		log.Fatalf("create: NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("create: node start failed: code %d", code)
	}
	defer func() {
		if code := node.Stop(); code != mesh.MOSS_OK {
			log.Printf("node stop: code %d", code)
		}
	}()

	iface := openIface(*useTun, *tunName)
	lan, err := meshlan.NewLanNode(node, *nick, *room, *cidr, iface)
	if err != nil {
		log.Fatalf("create: NewLanNode: %v", err)
	}
	if err := lan.Start(); err != nil {
		lan.Stop()
		log.Fatalf("create: LanNode start: %v", err)
	}
	defer lan.Stop()

	fmt.Printf("комната   %s\n", *room)
	fmt.Printf("ник       %s\n", *nick)
	fmt.Printf("peer ID   %s\n", peerIDOf(node))
	fmt.Printf("вирт. IP  %s\n", lan.SelfIP())
	fmt.Printf("cidr      %s\n", *cidr)

	// One invite per --invitee; with no --invitee, one addressed to our own
	// peer ID (the host's placeholder to hand around after learning IDs).
	targets := invitees
	if len(targets) == 0 {
		targets = []string{peerIDOf(node)}
		fmt.Fprintln(os.Stderr, "create: --invitee не задан; выпускаю инвайт для себя (замените на peerID гостя)")
	}
	for _, invitee := range targets {
		invite, err := lan.MakeInvite(invitee)
		if err != nil {
			log.Fatalf("create: инвайт для %s: %v", shortID(invitee), err)
		}
		fmt.Printf("инвайт для %s:\n  %s\n", shortID(invitee), invite)
	}

	fmt.Println("Ctrl-C — выход")
	runUntilSignal()
}

// runJoin: accept the invite, run until signal.
func runJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	nick := fs.String("nick", "", "ник (1–32 символа [a-zA-Z0-9_-])")
	inviteStr := fs.String("invite", "", "инвайт moss-lan://…")
	cidr := fs.String("cidr", defaultCIDR, "пул виртуальных IP")
	useTun := fs.Bool("tun", false, "реальный TUN-интерфейс вместо loopback")
	tunName := fs.String("tun-name", "", "имя TUN-интерфейса (пусто = авто)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("join: %v", err)
	}
	if *nick == "" || *inviteStr == "" {
		log.Fatal("join: --nick и --invite обязательны")
	}
	if err := meshlan.ValidateNick(*nick); err != nil {
		log.Fatalf("join: %v", err)
	}

	// Unpack before the node exists: the mesh ID inside the invite names
	// the room, and the node is born into it (empty PSK — the invite
	// carries the room key; AcceptRoomInvite installs it).
	meshID, _, err := meshlan.UnpackInvite(*inviteStr)
	if err != nil {
		log.Fatalf("join: %v", err)
	}

	node, err := mesh.NewNode(meshID, nil, isolatedConfig(meshID))
	if err != nil {
		log.Fatalf("join: NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("join: node start failed: code %d", code)
	}
	defer func() {
		if code := node.Stop(); code != mesh.MOSS_OK {
			log.Printf("node stop: code %d", code)
		}
	}()

	iface := openIface(*useTun, *tunName)
	lan, err := meshlan.NewLanNode(node, *nick, meshID, *cidr, iface)
	if err != nil {
		log.Fatalf("join: NewLanNode: %v", err)
	}

	// Accept the invite BEFORE LanNode.Start: AcceptRoomInvite installs
	// the room key, and the presence topic is an HMAC under that key —
	// subscribing first would resolve no topic and hear nothing.
	if err := lan.AcceptInvite(*inviteStr); err != nil {
		log.Fatalf("join: инвайт не принят: %v", err)
	}

	if err := lan.Start(); err != nil {
		lan.Stop()
		log.Fatalf("join: LanNode start: %v", err)
	}
	defer lan.Stop()

	fmt.Printf("комната   %s\n", meshID)
	fmt.Printf("ник       %s\n", *nick)
	fmt.Printf("peer ID   %s\n", peerIDOf(node))
	fmt.Printf("вирт. IP  %s\n", lan.SelfIP())
	fmt.Println("Ctrl-C — выход")
	runUntilSignal()
}

// runUntilSignal blocks until SIGINT/SIGTERM.
func runUntilSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Fprintln(os.Stderr, "\nвыход…")
}

// peerIDOf returns the node's own peer ID: hex of its Ed25519 public key
// (the core's pinned peer-ID format).
func peerIDOf(node *mesh.Node) string {
	pub := node.PublicKey()
	return hex.EncodeToString(pub[:])
}

// shortID abbreviates a peer ID for display.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12] + "…"
	}
	return id
}

// multiFlag collects repeated string flag values.
type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprint([]string(*m)) }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}
