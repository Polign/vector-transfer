// Package app exposes the standard Vector Transfer CLI and server to connector
// authors. Call Run from a small main package to include your own factories.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Polign/vector-transfer/accountauth"
	"github.com/Polign/vector-transfer/connector"
	"github.com/Polign/vector-transfer/control"
)

// Run executes the normal CLI, optionally adding third-party connector kinds.
// For serve, it owns connection cleanup and stops workers on SIGINT/SIGTERM.
func Run(args []string, factories ...connector.Factories) error {
	if len(args) == 0 {
		return errors.New("usage: vtransfer serve|worker|submit|jobs|show|events|cancel|resume|connections [flags] [job-id]")
	}
	if args[0] == "worker" {
		return runWorker(args[1:], factories...)
	}
	if args[0] == "serve" {
		return serve(args[1:], factories...)
	}
	return client(args[0], args[1:])
}

func serve(args []string, factories ...connector.Factories) error {
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := f.String("addr", "127.0.0.1:23005", "listen address")
	data := f.String("data", "data", "durable job data directory")
	config := f.String("config", "config.json", "connection configuration")
	accountConfig := f.String("account-config", "", "Polign account sign-in configuration (HTTPS deployment)")
	credentialKeyFile := f.String("credential-key-file", "", "base64 encryption key file for private user connections (account mode)")
	demo := f.Bool("demo", false, "use local sample source and file sink")
	workers := f.Int("workers", 2, "hosted concurrent jobs (0 disables hosted execution; maximum 32)")
	workerDownloads := f.String("worker-downloads", "", "directory of public worker binaries")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected serve argument")
	}
	if *workers < 0 || *workers > 32 {
		return errors.New("workers must be between 0 and 32")
	}
	token := os.Getenv("VECTOR_TRANSFER_TOKEN")
	var accounts *accountauth.Service
	if *accountConfig != "" {
		if token != "" || *demo {
			return errors.New("account mode cannot be combined with an operator token or demo connections")
		}
		var err error
		accounts, err = accountauth.Load(*accountConfig)
		if err != nil {
			return err
		}
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if accounts == nil && token == "" && (ip == nil || !ip.IsLoopback()) && host != "localhost" {
		return errors.New("set VECTOR_TRANSFER_TOKEN before listening beyond loopback")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := control.OpenStore(*data)
	if err != nil {
		return err
	}
	defer store.Close()
	var registry connector.Registry
	if *demo {
		registry, err = connector.DemoRegistry(*data)
	} else {
		registry, err = connector.LoadConfig(ctx, *config, factories...)
	}
	if err != nil {
		return err
	}
	defer registry.Close()
	engine := &control.Engine{Store: store, Registry: registry, WorkerDownloads: *workerDownloads, HostedDisabled: *workers == 0}
	engine.Workers, err = control.OpenWorkers(filepath.Join(*data, "workers"))
	if err != nil {
		return err
	}
	defer engine.Workers.Close()
	if *credentialKeyFile != "" {
		if accounts == nil {
			return errors.New("personal connections require Polign account mode")
		}
		engine.Connections, err = control.OpenConnections(filepath.Join(*data, "connections"), *credentialKeyFile, accounts.Realm())
		if err != nil {
			return err
		}
		defer engine.Connections.Close()
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	handler := control.Handler(engine, token)
	if accounts != nil {
		handler = accounts.SecureOrigin(control.AccountHandler(engine, accounts))
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	engineDone := make(chan error, 1)
	serverDone := make(chan error, 1)
	go func() {
		if *workers == 0 {
			<-ctx.Done()
			engineDone <- nil
			return
		}
		engineDone <- engine.Run(ctx, *workers)
	}()
	go func() { serverDone <- server.Serve(listener) }()
	log.Printf("Vector Transfer console: http://%s (connections: %d)", listener.Addr(), len(registry))
	engineFinished := false
	select {
	case <-ctx.Done():
	case err = <-engineDone:
		engineFinished = true
	case err = <-serverDone:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	if !engineFinished {
		workerErr := <-engineDone
		if err == nil {
			err = workerErr
		}
	}
	return err
}

func client(command string, args []string) error {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	base := f.String("url", "http://127.0.0.1:23005", "control plane URL")
	file := f.String("file", "", "job JSON file for submit")
	key := f.String("idempotency-key", "", "deduplicate job submissions")
	after := f.String("after", "0", "audit event sequence cursor")
	if err := f.Parse(args); err != nil {
		return err
	}
	method := "GET"
	path := ""
	var body []byte
	switch command {
	case "submit":
		method = "POST"
		path = "/v1/jobs"
		if *file == "" {
			return errors.New("submit requires -file job.json")
		}
		var err error
		body, err = os.ReadFile(*file)
		if err != nil {
			return err
		}
	case "jobs":
		path = "/v1/jobs"
	case "connections":
		path = "/v1/connections"
	case "show", "events", "cancel", "resume":
		if f.NArg() != 1 {
			return errors.New(command + " requires one job ID after flags")
		}
		path = "/v1/jobs/" + url.PathEscape(f.Arg(0))
		if command == "events" {
			path += "/events?limit=1000&after=" + url.QueryEscape(*after)
		}
		if command == "cancel" || command == "resume" {
			method = "POST"
			path += "/" + command
			body = []byte(`{}`)
		}
	default:
		return errors.New("unknown command: " + command)
	}
	req, err := http.NewRequest(method, strings.TrimRight(*base, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	token := os.Getenv("VECTOR_TRANSFER_ACCOUNT_TOKEN")
	if token == "" {
		token = os.Getenv("VECTOR_TRANSFER_TOKEN")
	}
	if token != "" {
		u := req.URL
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("tokens require HTTPS outside loopback")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if *key != "" {
		req.Header.Set("Idempotency-Key", *key)
	}
	httpClient := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, b, "", "  "); err != nil {
		return err
	}
	fmt.Println(pretty.String())
	return nil
}
