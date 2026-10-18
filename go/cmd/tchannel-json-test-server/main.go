// Test-only interoperability server using the repository's pinned tchannel-go.
package main

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"log"
	"strings"
	"time"

	"github.com/uber/tchannel-go"
	channeljson "github.com/uber/tchannel-go/json"
)

type request map[string]interface{}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address")
	flag.Parse()
	ch, err := tchannel.NewChannel("test-json", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()
	err = channeljson.Register(ch, channeljson.Handlers{
		"echo": func(ctx channeljson.Context, req request) (request, error) {
			return request{"request": req, "headers": ctx.Headers()}, nil
		},
		"large": func(_ channeljson.Context, _ request) (request, error) {
			return request{"value": strings.Repeat("x", 200000)}, nil
		},
		"app-error": func(_ channeljson.Context, _ request) (request, error) {
			return nil, fmt.Errorf("deterministic application error")
		},
		"delayed": func(ctx channeljson.Context, _ request) (request, error) {
			select {
			case <-time.After(2 * time.Second):
				return request{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}, func(_ context.Context, err error) { log.Printf("handler error: %v", err) })
	if err != nil {
		log.Fatal(err)
	}
	ch.Register(tchannel.HandlerFunc(func(_ context.Context, call *tchannel.InboundCall) {
		arg2, err := call.Response().Arg2Writer()
		if err != nil {
			_ = call.Response().SendSystemError(err)
			return
		}
		if err = tchannel.NewArgWriter(arg2, nil).Write([]byte("{}\n")); err != nil {
			_ = call.Response().SendSystemError(err)
			return
		}
		arg3, err := call.Response().Arg3Writer()
		if err != nil {
			_ = call.Response().SendSystemError(err)
			return
		}
		if err = tchannel.NewArgWriter(arg3, nil).Write([]byte("not-json\n")); err != nil {
			_ = call.Response().SendSystemError(err)
		}
	}), "bad-json")
	if err = ch.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
	log.Printf("listening %s", ch.PeerInfo().HostPort)
	select {}
}
