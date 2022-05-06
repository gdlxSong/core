package clickhouse

type Option struct {
	//Addrs     string `json:"addrs,omitempty"`
	Urls   []string         `json:"urls"`
	DbName string           `json:"dbName,omitempty"` //nolint
	Table  string           `json:"table,omitempty"`
	Fields map[string]Field `json:"fields,omitempty"`
}

type Field struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
type execNode struct {
	ts     int64
	fields []string
	args   []interface{}
}

const (
	ClickhouseSSQLTlp = `INSERT INTO %s.%s (%s) VALUES (%s)`
)
