package config

type PlacementConfig struct {
	Port int        `mapstructure:"port"`
	Raft RaftConfig `mapstructure:"raft"`
}

type RaftConfig struct {
	Servers      []PeerInfo `mapstructure:"servers"`
	LogStorePath string     `mapstructure:"log_store_path"`
}

type PeerInfo struct {
	ID   string `mapstructure:"id"`
	Addr string `mapstructure:"addr"`
}
