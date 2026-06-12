// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var (
		configPath     = flag.String("config", filepath.Join("config", "bft-smart", workloadFileName), "SmallBank XML config file or directory containing smallbank.xml")
		nodes          = flag.Int("nodes", 4, "number of SmartBFT nodes to run in-process")
		batchSize      = flag.Uint64("batch-size", 100, "maximum SmartBFT request batch size")
		batchTimeout   = flag.Duration("batch-timeout", 50*time.Millisecond, "maximum SmartBFT request batch interval")
		requestTimeout = flag.Duration("request-timeout", 30*time.Second, "timeout waiting for a request to be delivered")
		create         = flag.Bool("create", false, "create initial SmallBank accounts before executing")
		execute        = flag.Bool("execute", false, "execute workload phases")
		createWorkers  = flag.Int("create-workers", 32, "number of account creation workers")
		startUnixMS    = flag.Int64("start-unix-ms", 0, "absolute Unix epoch time in ms for benchmark start")
		failureSpec    = flag.String("failure-spec", "", "failure_spec.xml path; only pbft/proposalDelay is applied")
		failureStartMS = flag.Int64("failure-start-unix-ms", -1, "absolute Unix epoch time in ms for failure schedule start")
		dataDir        = flag.String("data-dir", "", "directory for SmartBFT WAL data; defaults to a temporary directory")
		keepData       = flag.Bool("keep-data", false, "keep generated WAL data when using a temporary data directory")
		verbose        = flag.Bool("verbose", false, "enable SmartBFT logs")
	)
	flag.Parse()

	if !*create && !*execute {
		*create = true
		*execute = true
	}

	cfg, err := loadWorkloadConfig(*configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	printConfig(cfg)

	failureStart := *failureStartMS
	if *failureSpec != "" && failureStart < 0 {
		if *startUnixMS > 0 {
			failureStart = *startUnixMS
		} else {
			failureStart = time.Now().UnixMilli()
		}
	}
	failures, err := loadProposalDelayController(*failureSpec, failureStart)
	if err != nil {
		fatalf("load failure spec: %v", err)
	}
	if failures.enabled {
		fmt.Printf("Failure injection enabled: spec=%s start_unix_ms=%d warm_up_ms=%d phases=%d\n",
			failures.specPath, failures.startUnixMS, failures.warmUp.Milliseconds(), len(failures.phases))
	}

	dir := *dataDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "smartbft-smallbank-")
		if err != nil {
			fatalf("create temp dir: %v", err)
		}
		if !*keepData {
			defer os.RemoveAll(dir)
		}
	}

	cluster, err := newCluster(*nodes, nodeOptions{
		NumNodes:     *nodes,
		BatchSize:    *batchSize,
		BatchTimeout: *batchTimeout,
		Failures:     failures,
	}, dir, *verbose)
	if err != nil {
		fatalf("start cluster: %v", err)
	}
	defer cluster.stop()

	if err := cluster.waitForLeader(5 * time.Second); err != nil {
		fatalf("%v", err)
	}

	bench := newBenchmarker(cluster, cfg, *requestTimeout)
	if *create {
		if err := bench.createAccounts(*createWorkers); err != nil {
			fatalf("create accounts: %v", err)
		}
	}
	if *execute {
		bench.execute(*startUnixMS)
	}

	cluster.waitForMatchingChecksums(5 * time.Second)
	printChecksums(cluster.stateChecksums())
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
