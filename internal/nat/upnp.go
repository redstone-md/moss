package nat

type PortMapper interface {
	Map(port int) (string, bool)
}

type NoopMapper struct{}

func (NoopMapper) Map(port int) (string, bool) {
	return "", false
}
