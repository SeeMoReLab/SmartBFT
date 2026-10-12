// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	var (
		role            = flag.String("role", "", "run role: server or client")
		nodeID          = flag.Uint64("node-id", 0, "SmartBFT node ID for --role server")
		hostsConfig     = flag.String("hosts-config", "", "SmartBFT hosts config for network server/client roles")
		configPath      = flag.String("config", filepath.Join("config", "smartbft", workloadFileName), "SmallBank XML config file or directory containing smallbank.xml")
		batchSize       = flag.Uint64("batch-size", 100, "maximum SmartBFT request batch size")
		batchTimeout    = flag.Duration("batch-timeout", 50*time.Millisecond, "maximum SmartBFT request batch interval")
		requestTimeout  = flag.Duration("request-timeout", 30*time.Second, "timeout waiting for a request to be delivered")
		replyListen     = flag.String("client-reply-listen", "127.0.0.1:0", "client reply listener address for broadcast mode")
		replyAdvertise  = flag.String("client-reply-advertise-host", "", "host/IP advertised to servers for client replies; empty uses listener address")
		execute         = flag.Bool("execute", false, "execute workload phases")
		startUnixMS     = flag.Int64("start-unix-ms", 0, "absolute Unix epoch time in ms for benchmark start")
		failureSpec     = flag.String("failure-spec", "", "failure_spec.xml path; only pbft/proposalDelay is applied")
		failureStartMS  = flag.Int64("failure-start-unix-ms", -1, "absolute Unix epoch time in ms for failure schedule start")
		learning        = flag.Bool("learning", false, "enable SmartBFT learning-agent reports and recommendation polling")
		agentTarget     = flag.String("agent-target", "", "learning agent gRPC target, for example 127.0.0.1:50051")
		initialTimeout  = flag.Duration("learning-initial-election-timeout", 5*time.Second, "initial SmartBFT timeout value reported to the learning agent")
		featureDuration = flag.Duration("learning-feature-duration", defaultLearningFeatureDuration, "feature collection duration")
		replyWait       = flag.Duration("learning-reply-wait", defaultLearningReplyWait, "time to wait before applying a recommendation")
		warmupDuration  = flag.Duration("learning-warmup-duration", defaultLearningWarmupDuration, "post-apply warm-up duration")
		rewardDuration  = flag.Duration("learning-reward-duration", defaultLearningRewardDuration, "reward collection duration")
		learningAnchor  = flag.Int64("learning-anchor-unix-ms", -1, "absolute Unix epoch time in ms of episode 1's apply instant (benchmark start plus spec warm-up); required with --learning")
		dataDir         = flag.String("data-dir", "", "directory for SmartBFT WAL data (required for --role server)")
		verbose         = flag.Bool("verbose", false, "enable SmartBFT debug logs")
	)
	flag.Parse()

	learningOptions := learningOptions{
		Enabled:         *learning,
		AgentTarget:     *agentTarget,
		InitialTimeout:  *initialTimeout,
		FeatureDuration: *featureDuration,
		ReplyWait:       *replyWait,
		WarmupDuration:  *warmupDuration,
		RewardDuration:  *rewardDuration,
		AnchorUnixMS:    *learningAnchor,
	}
	logMode := smallBankLogModeQuiet
	if *verbose {
		logMode = smallBankLogModeDebug
	}

	switch *role {
	case "server":
		runNetworkServer(*nodeID, *hostsConfig, *batchSize, *batchTimeout, *failureSpec, *failureStartMS, learningOptions, *dataDir, logMode)
	case "client":
		runNetworkClient(*configPath, *hostsConfig, *requestTimeout, *replyListen, *replyAdvertise, *execute, *startUnixMS)
	default:
		fatalf("unknown --role %q; expected server or client", *role)
	}
}

func runNetworkServer(nodeID uint64, hostsConfig string, batchSize uint64, batchTimeout time.Duration, failureSpec string, failureStartMS int64, learning learningOptions, dataDir string, logMode smallBankLogMode) {
	if nodeID == 0 {
		fatalf("--role server requires --node-id")
	}
	if hostsConfig == "" {
		fatalf("--role server requires --hosts-config")
	}
	if dataDir == "" {
		fatalf("--role server requires --data-dir")
	}
	hosts, err := loadNetworkHosts(hostsConfig)
	if err != nil {
		fatalf("load hosts config: %v", err)
	}
	if failureSpec != "" && failureStartMS < 0 {
		fatalf("--failure-spec requires --failure-start-unix-ms")
	}
	failures, err := loadProposalDelayController(failureSpec, failureStartMS)
	if err != nil {
		fatalf("load failure spec: %v", err)
	}
	printFailureConfig(failures)

	server, err := newNetworkNodeServer(nodeID, hosts, nodeOptions{
		BatchSize:       batchSize,
		BatchTimeout:    batchTimeout,
		Failures:        failures,
		LearningOptions: learning,
	}, dataDir, logMode)
	if err != nil {
		fatalf("start server: %v", err)
	}

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		<-stopCtx.Done()
		defer close(shutdownDone)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.close(shutdownCtx)
	}()
	if err := server.serve(); err != nil {
		fatalf("serve: %v", err)
	}
	if stopCtx.Err() != nil {
		<-shutdownDone
	}
}

func runNetworkClient(configPath string, hostsConfig string, requestTimeout time.Duration, replyListen string, replyAdvertise string, execute bool, startUnixMS int64) {
	if hostsConfig == "" {
		fatalf("--role client requires --hosts-config")
	}
	cfg, err := loadWorkloadConfig(configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	printConfig(cfg)
	probe, err := encodeRequest(request{ClientID: "terminal-0", ID: "1"})
	if err != nil {
		fatalf("encode request: %v", err)
	}
	fmt.Printf("request_size=%d encoded=%d\n", requestSizeBytes, len(probe))
	hosts, err := loadNetworkHosts(hostsConfig)
	if err != nil {
		fatalf("load hosts config: %v", err)
	}
	client, err := newNetworkSmallBankClient(hosts, replyListen, replyAdvertise)
	if err != nil {
		fatalf("create network client: %v", err)
	}
	defer client.close()
	if err := client.waitForServers(30 * time.Second); err != nil {
		fatalf("%v", err)
	}

	bench := newBenchmarker(client, cfg, requestTimeout)
	if execute {
		bench.execute(startUnixMS)
	}

	time.Sleep(2 * time.Second)
	printChecksums(client.stateChecksums())
}

func printFailureConfig(failures *proposalDelayController) {
	if failures.enabled {
		fmt.Printf("Failure injection enabled: spec=%s start_unix_ms=%d warm_up_ms=%d phases=%d\n",
			failures.specPath, failures.startUnixMS, failures.warmUp.Milliseconds(), len(failures.phases))
	}
}

func printConfig(cfg *workloadConfig) {
	fmt.Println("Configuration loaded:")
	fmt.Printf("Random seed: %d\n", cfg.RandomSeed)
	fmt.Printf("Monitor interval: %d seconds\n", cfg.MonitorIntervalSec)
	for i, phase := range cfg.Phases {
		fmt.Printf("Phase %d:\n", i+1)
		fmt.Printf("Duration: %d seconds\n", phase.DurationSec)
		fmt.Printf("Terminals: %d\n", phase.Terminals)
		if phase.Rate == 0 {
			fmt.Println("Rate: SATURATE (rate=0)")
		} else {
			fmt.Printf("Rate: %.2f TPS/terminal\n", phase.Rate)
		}
	}
}

func printChecksums(checksums map[uint64]string) {
	fmt.Println("State checksums:")
	var first string
	matches := true
	for id := uint64(1); id <= uint64(len(checksums)); id++ {
		checksum := checksums[id]
		fmt.Printf("node=%d checksum=%s\n", id, checksum)
		if first == "" {
			first = checksum
		} else if checksum != first {
			matches = false
		}
	}
	if matches {
		fmt.Println("All node checksums match.")
	} else {
		fmt.Println("WARNING: node checksums differ.")
	}
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
