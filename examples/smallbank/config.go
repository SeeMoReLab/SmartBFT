// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const workloadFileName = "smallbank.xml"

type workloadConfig struct {
	NumAccounts        uint64
	RandomSeed         int64
	MonitorIntervalSec int
	Phases             []phaseConfig
}

type phaseConfig struct {
	DurationSec int
	Terminals   int
	Rate        float64
	Weights     [6]int
}

type xmlParameters struct {
	NumAccounts     uint64   `xml:"numAccounts"`
	RandomSeed      int64    `xml:"randomSeed"`
	MonitorInterval int      `xml:"monitorInterval"`
	Works           xmlWorks `xml:"works"`
}

type xmlWorks struct {
	Work []xmlWork `xml:"work"`
}

type xmlWork struct {
	Time      int     `xml:"time"`
	Terminals int     `xml:"terminals"`
	Rate      float64 `xml:"rate"`
	Weights   string  `xml:"weights"`
}

func loadWorkloadConfig(path string) (*workloadConfig, error) {
	if path == "" {
		path = filepath.Join("config", workloadFileName)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		path = filepath.Join(path, workloadFileName)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var params xmlParameters
	if err := xml.Unmarshal(raw, &params); err != nil {
		return nil, err
	}

	cfg := &workloadConfig{
		NumAccounts:        params.NumAccounts,
		RandomSeed:         params.RandomSeed,
		MonitorIntervalSec: params.MonitorInterval,
		Phases:             make([]phaseConfig, 0, len(params.Works.Work)),
	}
	if cfg.NumAccounts == 0 {
		cfg.NumAccounts = 100000
	}
	if cfg.RandomSeed == 0 {
		cfg.RandomSeed = 17
	}
	if len(params.Works.Work) == 0 {
		return nil, fmt.Errorf("no workload phases found in %s", path)
	}

	for idx, work := range params.Works.Work {
		if work.Time <= 0 {
			return nil, fmt.Errorf("phase %d has non-positive time: %d", idx+1, work.Time)
		}
		if work.Terminals <= 0 {
			return nil, fmt.Errorf("phase %d has non-positive terminals: %d", idx+1, work.Terminals)
		}
		if work.Rate < 0 {
			return nil, fmt.Errorf("phase %d has negative rate: %f", idx+1, work.Rate)
		}
		weights, err := parseWeights(work.Weights)
		if err != nil {
			return nil, fmt.Errorf("phase %d: %w", idx+1, err)
		}
		cfg.Phases = append(cfg.Phases, phaseConfig{
			DurationSec: work.Time,
			Terminals:   work.Terminals,
			Rate:        work.Rate,
			Weights:     weights,
		})
	}

	return cfg, nil
}

func parseWeights(raw string) ([6]int, error) {
	var weights [6]int
	if strings.TrimSpace(raw) == "" {
		raw = "15,15,25,15,15,15"
	}
	parts := strings.Split(raw, ",")
	if len(parts) != len(weights) {
		return weights, fmt.Errorf("weights must contain %d values", len(weights))
	}

	var total int
	for i, part := range parts {
		val, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return weights, fmt.Errorf("invalid weight %q: %w", part, err)
		}
		if val < 0 {
			return weights, fmt.Errorf("weights must be non-negative")
		}
		weights[i] = val
		total += val
	}
	if total != 100 {
		return weights, fmt.Errorf("weights must sum to 100, got %d", total)
	}
	return weights, nil
}
