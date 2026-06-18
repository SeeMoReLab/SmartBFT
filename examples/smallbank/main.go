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
		role           = flag.String("role", "inprocess", "run role: inprocess, server, or client")
		nodeID         = flag.Uint64("node-id", 0, "SmartBFT node ID for --role server")
		hostsConfig    = flag.String("hosts-config", "", "SmartBFT hosts config for network server/client roles")
		configPath     = flag.String("config", filepath.Join("config", "bft-smart", workloadFileName), "SmallBank XML config file or directory containing smallbank.xml")
		nodes          = flag.Int("nodes", 4, "number of SmartBFT nodes to run in-process")
		batchSize      = flag.Uint64("batch-size", 100, "maximum SmartBFT request batch size")
		batchTimeout   = flag.Duration("batch-timeout", 50*time.Millisecond, "maximum SmartBFT request batch interval")
		requestTimeout = flag.Duration("request-timeout", 30*time.Second, "timeout waiting for a request to be delivered")
		submitModeFlag = flag.String("submit-mode", string(submitModeBroadcast), "network client submit mode: broadcast or leader")
		replyListen    = flag.String("client-reply-listen", "127.0.0.1:0", "client reply listener address for broadcast mode")
		replyAdvertise = flag.String("client-reply-advertise-host", "", "host/IP advertised to servers for client replies; empty uses listener address")
		create         = flag.Bool("create", false, "create initial SmallBank accounts before executing")
		execute        = flag.Bool("execute", false, "execute workload phases")
		createWorkers  = flag.Int("create-workers", 0, "number of account creation workers; 0 uses the first workload phase terminals")
		startUnixMS    = flag.Int64("start-unix-ms", 0, "absolute Unix epoch time in ms for benchmark start")
		failureSpec    = flag.String("failure-spec", "", "failure_spec.xml path; only pbft/proposalDelay is applied")
		failureStartMS = flag.Int64("failure-start-unix-ms", -1, "absolute Unix epoch time in ms for failure schedule start")
		learning       = flag.Bool("learning", false, "enable PBFT learning-agent reports and recommendation polling")
		learningNodeID = flag.Uint64("learning-node-id", 1, "SmartBFT node ID that sends learning reports")
		agentTarget    = flag.String("agent-target", "", "learning agent gRPC target, for example 127.0.0.1:50051")
		initialTimeout = flag.Duration("learning-initial-election-timeout", 5*time.Second, "initial PBFT timeout value reported to the learning agent")
		reportTicks    = flag.Uint64("learning-report-tick-interval", defaultLearningReportTickInterval, "consensus ticks between learning report checks")
		reportTrigger  = flag.Duration("learning-report-trigger", defaultLearningReportTrigger, "minimum elapsed time before the first learning report")
		maxReportLen   = flag.Uint64("learning-max-report-length", defaultLearningMaxReportLength, "maximum consensus ticks in one learning report window")
		pollInterval   = flag.Duration("learning-poll-interval", defaultLearningPollInterval, "interval for polling the learning agent for timeout recommendations")
		rpcTimeout     = flag.Duration("learning-rpc-timeout", defaultLearningRPCTimeout, "timeout for learning agent RPCs")
		dataDir        = flag.String("data-dir", "", "directory for SmartBFT WAL data; defaults to a temporary directory")
		keepData       = flag.Bool("keep-data", false, "keep generated WAL data when using a temporary data directory")
		verbose        = flag.Bool("verbose", false, "enable SmartBFT logs")
	)
	flag.Parse()

	mode, err := parseSubmitMode(*submitModeFlag)
	if err != nil {
		fatalf("%v", err)
	}

	if *role == "inprocess" && !*create && !*execute {
		*create = true
		*execute = true
	}

	learningOptions := learningOptions{
		Enabled:            *learning,
		NodeID:             *learningNodeID,
		AgentTarget:        *agentTarget,
		InitialTimeout:     *initialTimeout,
		ReportTickInterval: *reportTicks,
		ReportTrigger:      *reportTrigger,
		MaxReportLength:    *maxReportLen,
		PollInterval:       *pollInterval,
		RPCTimeout:         *rpcTimeout,
	}

	switch *role {
	case "inprocess":
		runInProcess(*configPath, *nodes, *batchSize, *batchTimeout, *requestTimeout, *create, *execute, *createWorkers, *startUnixMS, *failureSpec, *failureStartMS, learningOptions, *dataDir, *keepData, *verbose)
	case "server":
		runNetworkServer(*nodeID, *hostsConfig, *batchSize, *batchTimeout, *requestTimeout, *failureSpec, *failureStartMS, *startUnixMS, learningOptions, *dataDir, *keepData, *verbose)
	case "client":
		if !*create && !*execute {
			*create = true
			*execute = true
		}
		runNetworkClient(*configPath, *hostsConfig, *requestTimeout, mode, *replyListen, *replyAdvertise, *create, *execute, *createWorkers, *startUnixMS)
	default:
		fatalf("unknown --role %q; expected inprocess, server, or client", *role)
	}
}

func runInProcess(configPath string, nodes int, batchSize uint64, batchTimeout time.Duration, requestTimeout time.Duration, create bool, execute bool, createWorkers int, startUnixMS int64, failureSpec string, failureStartMS int64, learning learningOptions, dataDir string, keepData bool, verbose bool) {
	cfg, err := loadWorkloadConfig(configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	printConfig(cfg)

	failures, err := loadFailures(failureSpec, failureStartMS, startUnixMS)
	if err != nil {
		fatalf("load failure spec: %v", err)
	}
	printFailureConfig(failures)

	dir := dataDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "smartbft-smallbank-")
		if err != nil {
			fatalf("create temp dir: %v", err)
		}
		if !keepData {
			defer os.RemoveAll(dir)
		}
	}

	cluster, err := newCluster(nodes, nodeOptions{
		NumNodes:        nodes,
		BatchSize:       batchSize,
		BatchTimeout:    batchTimeout,
		Failures:        failures,
		LearningOptions: learning,
	}, dir, verbose)
	if err != nil {
		fatalf("start cluster: %v", err)
	}
	defer cluster.stop()

	if err := cluster.waitForLeader(5 * time.Second); err != nil {
		fatalf("%v", err)
	}

	bench := newBenchmarker(cluster, cfg, requestTimeout)
	if create {
		if err := bench.createAccounts(accountCreationWorkers(createWorkers, cfg)); err != nil {
			fatalf("create accounts: %v", err)
		}
	}
	if execute {
		bench.execute(startUnixMS)
	}

	cluster.waitForMatchingChecksums(5 * time.Second)
	printChecksums(cluster.stateChecksums())
}

func runNetworkServer(nodeID uint64, hostsConfig string, batchSize uint64, batchTimeout time.Duration, requestTimeout time.Duration, failureSpec string, failureStartMS int64, startUnixMS int64, learning learningOptions, dataDir string, keepData bool, verbose bool) {
	if nodeID == 0 {
		fatalf("--role server requires --node-id")
	}
	if hostsConfig == "" {
		fatalf("--role server requires --hosts-config")
	}
	hosts, err := loadNetworkHosts(hostsConfig)
	if err != nil {
		fatalf("load hosts config: %v", err)
	}
	failures, err := loadFailures(failureSpec, failureStartMS, startUnixMS)
	if err != nil {
		fatalf("load failure spec: %v", err)
	}
	printFailureConfig(failures)

	dir := dataDir
	if dir == "" {
		dir, err = os.MkdirTemp("", fmt.Sprintf("smartbft-smallbank-node-%d-", nodeID))
		if err != nil {
			fatalf("create temp dir: %v", err)
		}
		if !keepData {
			defer os.RemoveAll(dir)
		}
	}

	server, err := newNetworkNodeServer(nodeID, hosts, nodeOptions{
		NumNodes:        len(hosts),
		BatchSize:       batchSize,
		BatchTimeout:    batchTimeout,
		Failures:        failures,
		LearningOptions: learning,
	}, dir, verbose, requestTimeout)
	if err != nil {
		fatalf("start server: %v", err)
	}

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.close(shutdownCtx)
	}()
	if err := server.serve(); err != nil {
		fatalf("serve: %v", err)
	}
}

func runNetworkClient(configPath string, hostsConfig string, requestTimeout time.Duration, submitMode submitMode, replyListen string, replyAdvertise string, create bool, execute bool, createWorkers int, startUnixMS int64) {
	if hostsConfig == "" {
		fatalf("--role client requires --hosts-config")
	}
	cfg, err := loadWorkloadConfig(configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	printConfig(cfg)
	hosts, err := loadNetworkHosts(hostsConfig)
	if err != nil {
		fatalf("load hosts config: %v", err)
	}
	client, err := newNetworkSmallBankClient(hosts, requestTimeout, submitMode, replyListen, replyAdvertise)
	if err != nil {
		fatalf("create network client: %v", err)
	}
	defer client.close()
	fmt.Printf("Submit mode: %s\n", submitMode)
	if err := client.waitForServers(30 * time.Second); err != nil {
		fatalf("%v", err)
	}

	bench := newBenchmarker(client, cfg, requestTimeout)
	if create {
		if err := bench.createAccounts(accountCreationWorkers(createWorkers, cfg)); err != nil {
			fatalf("create accounts: %v", err)
		}
	}
	if execute {
		bench.execute(startUnixMS)
	}

	time.Sleep(2 * time.Second)
	printChecksums(client.stateChecksums())
}

func loadFailures(failureSpec string, failureStartMS int64, startUnixMS int64) (*proposalDelayController, error) {
	failureStart := failureStartMS
	if failureSpec != "" && failureStart < 0 {
		if startUnixMS > 0 {
			failureStart = startUnixMS
		} else {
			failureStart = time.Now().UnixMilli()
		}
	}
	return loadProposalDelayController(failureSpec, failureStart)
}

func printFailureConfig(failures *proposalDelayController) {
	if failures.enabled {
		fmt.Printf("Failure injection enabled: spec=%s start_unix_ms=%d warm_up_ms=%d phases=%d\n",
			failures.specPath, failures.startUnixMS, failures.warmUp.Milliseconds(), len(failures.phases))
	}
}

func printConfig(cfg *workloadConfig) {
	fmt.Println("Configuration loaded:")
	fmt.Printf("Accounts: %d\n", cfg.NumAccounts)
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
		fmt.Printf("Weights: %v\n", phase.Weights)
	}
}

func accountCreationWorkers(configuredWorkers int, cfg *workloadConfig) int {
	if configuredWorkers > 0 {
		return configuredWorkers
	}
	return cfg.Phases[0].Terminals
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
