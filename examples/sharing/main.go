// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

// Command sharing runs the report chain used by the learning agent's sharing
// mode. Each node runs one sharing server next to its learning agent. The agent
// collects 2f+1 signed reports for an episode and submits them here; the report
// chain orders one batch per episode and hands the agreed batch back to every
// agent, so all nodes feed identical features to their models.
//
// The report chain is deliberately independent of the workload chain: it has
// its own ports, its own WAL, its own leader, and fixed timeouts that are never
// tuned by the recommendations it carries.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	var (
		role        = flag.String("role", "server", "run role: server or inprocess")
		nodeID      = flag.Uint64("node-id", 0, "sharing node id for --role server")
		hostsConfig = flag.String("hosts-config", "", "sharing hosts config: node_id host report_port agent_port consensus_port")
		protocolRaw = flag.String("protocol", "pbft", "consensus protocol being learned: pbft, sbft, or tendermint")
		dataDir     = flag.String("data-dir", "", "directory for report chain WAL data; defaults to a temporary directory")
		keepData    = flag.Bool("keep-data", false, "keep generated WAL data when using a temporary data directory")
		verbose     = flag.Bool("verbose", false, "enable SmartBFT debug logs for the report chain")
	)
	flag.Parse()

	protocol, err := parseProtocol(*protocolRaw)
	if err != nil {
		fatalf("%v", err)
	}
	if *hostsConfig == "" {
		fatalf("--hosts-config is required")
	}
	hosts, err := loadSharingHosts(*hostsConfig)
	if err != nil {
		fatalf("load sharing hosts config: %v", err)
	}

	dir := *dataDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "smartbft-sharing-")
		if err != nil {
			fatalf("create temp dir: %v", err)
		}
		if !*keepData {
			defer os.RemoveAll(dir)
		}
	}

	logger := newSharingLogger(*verbose)

	switch *role {
	case "server":
		if *nodeID == 0 {
			fatalf("--role server requires --node-id")
		}
		runServer(*nodeID, hosts, protocol, dir, logger)
	case "inprocess":
		runInProcess(hosts, protocol, dir, logger)
	default:
		fatalf("unknown --role %q; expected server or inprocess", *role)
	}
}

func runServer(nodeID uint64, hosts []hostEntry, protocol adaptivetimers.Protocol, dataDir string, logger smart.Logger) {
	server, err := newSharingServer(sharingOptions{
		ID:       nodeID,
		Hosts:    hosts,
		Protocol: protocol,
		DataDir:  dataDir,
		Logger:   logger,
	})
	if err != nil {
		fatalf("start sharing server: %v", err)
	}

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopCtx.Done()
		server.close()
	}()
	if err := server.serve(); err != nil {
		fatalf("serve: %v", err)
	}
}

// runInProcess starts every node from the hosts config in a single process.
// This is the local testing path: one process plus one learning agent per node
// is enough to exercise the whole sharing loop without a cluster.
func runInProcess(hosts []hostEntry, protocol adaptivetimers.Protocol, dataDir string, logger smart.Logger) {
	servers := make([]*sharingServer, 0, len(hosts))
	for _, host := range hosts {
		server, err := newSharingServer(sharingOptions{
			ID:       host.ID,
			Hosts:    hosts,
			Protocol: protocol,
			DataDir:  dataDir,
			Logger:   logger,
		})
		if err != nil {
			for _, started := range servers {
				started.close()
			}
			fatalf("start sharing node %d: %v", host.ID, err)
		}
		servers = append(servers, server)
	}

	var wg sync.WaitGroup
	for _, server := range servers {
		wg.Add(1)
		go func(server *sharingServer) {
			defer wg.Done()
			if err := server.serve(); err != nil {
				fmt.Fprintf(os.Stderr, "sharing node %d serve: %v\n", server.host.ID, err)
			}
		}(server)
	}

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	<-stopCtx.Done()
	for _, server := range servers {
		server.close()
	}
	wg.Wait()
}

func parseProtocol(raw string) (adaptivetimers.Protocol, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "pbft":
		return adaptivetimers.Protocol_PROTOCOL_PBFT, nil
	case "sbft":
		return adaptivetimers.Protocol_PROTOCOL_SBFT, nil
	case "tendermint":
		return adaptivetimers.Protocol_PROTOCOL_TENDERMINT, nil
	default:
		return adaptivetimers.Protocol_PROTOCOL_UNSPECIFIED,
			fmt.Errorf("unknown --protocol %q; expected pbft, sbft, or tendermint", raw)
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func newSharingLogger(verbose bool) smart.Logger {
	if verbose {
		zapLogger, _ := zap.NewDevelopment()
		return zapLogger.Sugar()
	}
	level := zap.NewAtomicLevelAt(zapcore.PanicLevel)
	writer := zapcore.AddSync(discardWriter{})
	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(zapcore.Lock(writer)),
		level,
	))
	return zapLogger.Sugar()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
