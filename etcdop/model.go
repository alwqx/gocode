package etcdop

const (
	DemoKey   = "foo"
	DemoNum   = 1000 // 1000条测试数据
	BatchSize = 20   // 并发操作大小
	// DefaultEtcdMaxTxnOpNum etcd批量操作中允许的最大key数量
	// refer to https://github.com/etcd-io/etcd/blob/main/server/embed/config.go#L58
	DefaultEtcdMaxTxnOpNum = 128
)

type KeyValue struct {
	Key   string
	Value string
}

type KeyModRev struct {
	Key    string
	ModRev int64
}
