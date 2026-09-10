package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Polign/vector-transfer/connector"
	"github.com/Polign/vector-transfer/worker"
)

func runWorker(args []string, factories ...connector.Factories) error {
	if len(args) == 0 {
		return errors.New("usage: vtransfer worker enroll|run [flags]")
	}
	command := args[0]
	f := flag.NewFlagSet("worker "+command, flag.ContinueOnError)
	data := f.String("data", "worker-data", "persistent private worker identity and checkpoint directory")
	base := f.String("url", "https://transfer.polign.com", "control plane origin (enrollment only)")
	tokenFile := f.String("token-file", "", "file containing the single-use enrollment token")
	config := f.String("config", "worker.json", "local worker configuration (run only)")
	allowHTTP := f.Bool("allow-http-loopback", false, "allow HTTP to loopback for local development only")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected worker argument")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := worker.Open(*data)
	if err != nil {
		return err
	}
	defer c.Close()
	switch command {
	case "enroll":
		if *tokenFile == "" {
			return errors.New("enrollment requires -token-file; do not put tokens on the command line")
		}
		info, err := os.Stat(*tokenFile)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 1024 || info.Mode().Perm()&0077 != 0 {
			return errors.New("enrollment token file must be private (0600) and at most 1 KiB")
		}
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			return err
		}
		if err = c.Enroll(ctx, *base, strings.TrimSpace(string(b)), *allowHTTP); err != nil {
			return err
		}
		fmt.Println("Worker enrolled. Remove the enrollment token file, then run the worker with its local configuration.")
		return nil
	case "run":
		cfg, err := worker.LoadConfig(*config)
		if err != nil {
			return err
		}
		registry, err := connector.LoadConfig(ctx, cfg.ConnectionsFile, factories...)
		if err != nil {
			return errors.New("could not open local connections; check configuration and locally supplied credentials")
		}
		defer registry.Close()
		fmt.Println("Worker starting. Database credentials and checkpoints stay in this data environment.")
		return c.Run(ctx, registry, cfg.Pairs, *allowHTTP)
	default:
		return errors.New("unknown worker command")
	}
}
