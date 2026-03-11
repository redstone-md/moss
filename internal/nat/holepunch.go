package nat

type EndpointPair struct {
	Local  string
	Remote string
}

type Coordinator struct {
	Attempts int
}

func (c Coordinator) Plan(local, remote string) []EndpointPair {
	out := make([]EndpointPair, 0, c.Attempts)
	for i := 0; i < c.Attempts; i++ {
		out = append(out, EndpointPair{Local: local, Remote: remote})
	}
	return out
}
