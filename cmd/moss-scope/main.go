// Command moss-scope is the observability binary for Moss: the public network
// explorer and the local debugger, in one program, serving one bundle.
//
// It replaces moss-gateway. `serve` does everything the gateway did — runs a
// relaying node on the shared substrate and publishes aggregate, privacy-
// preserving telemetry — and additionally serves the MossScope interface it is
// embedded with. `attach` is the other half: a loopback-only UI pointed at a
// node's debug port.
//
// The two modes do NOT share a route table. A public server has no handler for
// an inspection route, so it cannot be talked into serving one by a config
// mistake; see mux_test.go, which asserts exactly that.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// version is stamped at build time (-ldflags "-X main.version=..."). A binary
// that cannot say which build it is turns "did the deploy go out?" into a
// guess, which is the wrong kind of question to have during an incident.
var version = "dev"

func main() {
	log.SetFlags(0)
	log.SetPrefix("moss-scope: ")

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "attach":
		runAttach(os.Args[2:])
	case "run":
		runRun(os.Args[2:])
	case "version":
		fmt.Printf("moss-scope %s · protocol 1\n", version)
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `moss-scope — наблюдаемость Moss

  moss-scope serve     публичный узел-релей + агрегатная телеметрия + веб-интерфейс
  moss-scope attach    локальный отладчик: интерфейс на 127.0.0.1 поверх debug-порта узла
  moss-scope run       поднять узел с открытой отладкой и интерфейсом — одна команда, одна ссылка
  moss-scope version

Публичный режим не содержит маршрутов инспекции: они не зарегистрированы,
а не закрыты правами. Отладка доступна только на лупбэке и только с токеном.
`)
}

// serveHTTP starts srv and blocks until SIGINT/SIGTERM, then drains.
func serveHTTP(srv *http.Server, what string) {
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("%s: %v", what, err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("%s: останов", what)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
