// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// hostEntry describes one sharing server and the learning agent it serves.
//
// The report chain uses two listeners per node:
//   - reportPort carries report-chain consensus traffic between sharing servers.
//   - consensusPort exposes Consensus/SubmitReportBatch to the local learning
//     agent, and must match column 6 of the agent's own hosts config.
type hostEntry struct {
	ID            uint64
	Host          string
	ReportPort    int
	AgentPort     int
	ConsensusPort int
}

func (h hostEntry) reportAddress() string {
	return net.JoinHostPort(h.Host, strconv.Itoa(h.ReportPort))
}

func (h hostEntry) agentAddress() string {
	return net.JoinHostPort(h.Host, strconv.Itoa(h.AgentPort))
}

func (h hostEntry) consensusAddress() string {
	return net.JoinHostPort(h.Host, strconv.Itoa(h.ConsensusPort))
}

// loadSharingHosts parses a whitespace separated config with the columns
// node_id host report_port agent_port consensus_port.
func loadSharingHosts(path string) ([]hostEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var hosts []hostEntry
	for lineNo, rawLine := range strings.Split(string(raw), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 5 {
			return nil, fmt.Errorf("%s:%d: expected 5 columns: node_id host report_port agent_port consensus_port", path, lineNo+1)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%s:%d: invalid sharing node id %q", path, lineNo+1, parts[0])
		}
		ports := make([]int, 0, 3)
		for _, raw := range parts[2:5] {
			port, err := strconv.Atoi(raw)
			if err != nil || port <= 0 || port > 65535 {
				return nil, fmt.Errorf("%s:%d: invalid port %q", path, lineNo+1, raw)
			}
			ports = append(ports, port)
		}
		hosts = append(hosts, hostEntry{
			ID:            id,
			Host:          parts[1],
			ReportPort:    ports[0],
			AgentPort:     ports[1],
			ConsensusPort: ports[2],
		})
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no sharing hosts found in %s", path)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].ID < hosts[j].ID })
	for i := 1; i < len(hosts); i++ {
		if hosts[i-1].ID == hosts[i].ID {
			return nil, fmt.Errorf("duplicate sharing node id %d in %s", hosts[i].ID, path)
		}
	}
	return hosts, nil
}

func hostByID(hosts []hostEntry, id uint64) (hostEntry, bool) {
	for _, host := range hosts {
		if host.ID == id {
			return host, true
		}
	}
	return hostEntry{}, false
}

func nodeIDs(hosts []hostEntry) []uint64 {
	ids := make([]uint64, 0, len(hosts))
	for _, host := range hosts {
		ids = append(ids, host.ID)
	}
	return ids
}

// quorum returns 2f+1 for the given cluster size.
func quorum(numNodes int) int {
	f := (numNodes - 1) / 3
	return 2*f + 1
}
