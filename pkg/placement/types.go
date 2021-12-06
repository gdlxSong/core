package placement

type PlacementConfig struct {
	IpAddr string
}

type Placement interface {
	Register()
}
