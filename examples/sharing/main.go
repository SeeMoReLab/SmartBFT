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
	"syscall"

	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	var (
		nodeID      = flag.Uint64("node-id", 0, "sharing node id")
		hostsConfig = flag.String("hosts-config", "", "sharing hosts config: node_id host report_port agent_port consensus_port")
		protocolRaw = flag.String("protocol", "pbft", "consensus protocol being learned: pbft, smartbft, sbft, tendermint, hotstuff, or autobahn")
		dataDir     = flag.String("data-dir", "", "directory for report chain WAL data (required)")
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

	if *nodeID == 0 {
		fatalf("--node-id is required")
	}
	if *dataDir == "" {
		fatalf("--data-dir is required")
	}
	runServer(*nodeID, hosts, protocol, *dataDir, newSharingLogger())
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

func parseProtocol(raw string) (adaptivetimers.Protocol, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "pbft":
		return adaptivetimers.Protocol_PROTOCOL_PBFT, nil
	case "smartbft":
		return adaptivetimers.Protocol_PROTOCOL_SMARTBFT, nil
	case "sbft":
		return adaptivetimers.Protocol_PROTOCOL_SBFT, nil
	case "tendermint":
		return adaptivetimers.Protocol_PROTOCOL_TENDERMINT, nil
	case "hotstuff":
		return adaptivetimers.Protocol_PROTOCOL_HOTSTUFF, nil
	case "autobahn":
		return adaptivetimers.Protocol_PROTOCOL_AUTOBAHN, nil
	default:
		return adaptivetimers.Protocol_PROTOCOL_UNSPECIFIED,
			fmt.Errorf("unknown --protocol %q; expected pbft, smartbft, sbft, tendermint, hotstuff, or autobahn", raw)
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// newSharingLogger discards SmartBFT's own logging; the report chain prints its
// progress lines directly.
func newSharingLogger() smart.Logger {
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
