package nat

type PCPMapper struct{}

func (PCPMapper) Map(port int) (string, bool) {
	return "", false
}
