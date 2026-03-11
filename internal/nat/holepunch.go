package nat

import (
	"net"
	"strconv"
)

type EndpointPair struct {
	Local  string
	Remote string
}

type Coordinator struct {
	Attempts           int
	EnablePrediction   bool
	LocalObservations  []string
	RemoteObservations []string
}

func (c Coordinator) Plan(local, remote string) []EndpointPair {
	attempts := c.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	locals := []string{local}
	remotes := []string{remote}
	if c.EnablePrediction {
		locals = predictedEndpoints(local, c.LocalObservations, attempts)
		remotes = predictedEndpoints(remote, c.RemoteObservations, attempts)
	}
	out := make([]EndpointPair, 0, attempts)
	for i := 0; i < attempts; i++ {
		out = append(out, EndpointPair{
			Local:  locals[min(i, len(locals)-1)],
			Remote: remotes[min(i, len(remotes)-1)],
		})
	}
	return out
}

func predictedEndpoints(base string, observations []string, attempts int) []string {
	if attempts <= 0 {
		return nil
	}
	host, port, err := net.SplitHostPort(base)
	if err != nil {
		return repeatEndpoint(base, attempts)
	}
	basePort, err := strconv.Atoi(port)
	if err != nil {
		return repeatEndpoint(base, attempts)
	}
	step := predictPortStep(observations)
	if step == 0 {
		return repeatEndpoint(net.JoinHostPort(host, strconv.Itoa(basePort)), attempts)
	}
	out := make([]string, 0, attempts)
	seen := make(map[string]struct{}, attempts)
	for i := 0; i < attempts; i++ {
		candidate := net.JoinHostPort(host, strconv.Itoa(basePort+(step*i)))
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	for len(out) < attempts {
		out = append(out, out[len(out)-1])
	}
	return out
}

func predictPortStep(observations []string) int {
	ports := observedPorts(observations)
	if len(ports) < 2 {
		return 0
	}
	stepCounts := make(map[int]int)
	bestStep := 0
	bestCount := 0
	for i := 1; i < len(ports); i++ {
		step := ports[i] - ports[i-1]
		if step == 0 {
			continue
		}
		stepCounts[step]++
		if stepCounts[step] > bestCount {
			bestCount = stepCounts[step]
			bestStep = step
		}
	}
	return bestStep
}

func observedPorts(observations []string) []int {
	out := make([]int, 0, len(observations))
	last := -1
	for _, observed := range observations {
		_, port, err := net.SplitHostPort(observed)
		if err != nil {
			continue
		}
		value, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		if value == last {
			continue
		}
		last = value
		out = append(out, value)
	}
	return out
}

func repeatEndpoint(addr string, attempts int) []string {
	out := make([]string, 0, attempts)
	for i := 0; i < attempts; i++ {
		out = append(out, addr)
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
