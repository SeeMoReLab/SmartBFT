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
)

const workloadFileName = "smallbank.xml"

type workloadConfig struct {
	RandomSeed         int64
	MonitorIntervalSec int
	Phases             []phaseConfig
}

type phaseConfig struct {
	DurationSec int
	Terminals   int
	Rate        float64
}

type xmlParameters struct {
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
}

func loadWorkloadConfig(path string) (*workloadConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("workload config path is empty")
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
		RandomSeed:         params.RandomSeed,
		MonitorIntervalSec: params.MonitorInterval,
		Phases:             make([]phaseConfig, 0, len(params.Works.Work)),
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
		cfg.Phases = append(cfg.Phases, phaseConfig{
			DurationSec: work.Time,
			Terminals:   work.Terminals,
			Rate:        work.Rate,
		})
	}

	return cfg, nil
}
