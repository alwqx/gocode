package etcdop

import (
	"context"
	"fmt"
	"log"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClient struct {
	Client *clientv3.Client
}

func (etcd *EtcdClient) PutDemoData() error {
	return nil
}

func (etcd *EtcdClient) LoopPut(kvs []KeyValue) error {
	for _, kv := range kvs {
		_, err := etcd.Client.Put(context.TODO(), kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (etcd *EtcdClient) ConcurrentPut(kvs []KeyValue) error {
	num := len(kvs)
	if num == 0 {
		return nil
	}

	loop := num / BatchSize
	if num%BatchSize != 0 {
		loop += 1
	}

	var wg sync.WaitGroup
	for i := 0; i < loop; i++ {
		for j := i; j < num && i+BatchSize+j < num; j++ {
			kv := kvs[j]
			wg.Add(1)
			go func() {
				_, err := etcd.Client.Put(context.TODO(), kv.Key, kv.Value)
				if err != nil {
					log.Fatalf("[etcd] put key=%s value=%s error", kv.Key, kv.Value)
				}
			}()

			wg.Done()
		}
		wg.Wait()
	}

	return nil
}

func (etcd *EtcdClient) BatchPut(kvs []KeyValue) error {
	num := len(kvs)
	if num == 0 {
		return nil
	}

	totOps, _, err := BuildEtcdPutOps(kvs)
	if err != nil {
		return err
	}

	for _, ops := range totOps {
		tx := etcd.Client.Txn(context.TODO())
		_, err = tx.Then(ops...).Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// BuildEtcdPutOps
func BuildEtcdPutOps(kvs []KeyValue) ([][]clientv3.Op, int, error) {
	tot := len(kvs)
	if tot == 0 {
		return nil, 0, fmt.Errorf("kvs is empty")
	}

	// 计算多少组批量请求，每组批量请求不能超过 etcd 限制的事务单次最大请求量 DefaultMaxTxnOpNum
	batchSize := DefaultEtcdMaxTxnOpNum
	groupLen := tot / batchSize
	if tot%batchSize != 0 {
		groupLen++
	}

	// 构造批量 get 请求
	totOps := make([][]clientv3.Op, 0, groupLen)
	ops := make([]clientv3.Op, 0, batchSize)
	cnt := 0
	for _, kv := range kvs {
		if cnt == batchSize {
			totOps = append(totOps, ops)
			cnt = 0
			ops = make([]clientv3.Op, 0, batchSize)
		}

		cnt++
		ops = append(ops, clientv3.OpPut(kv.Key, kv.Value))
	}
	if len(ops) > 0 {
		totOps = append(totOps, ops)
	}

	return totOps, batchSize, nil
}

// BuildEtcdGetOps
func BuildEtcdGetOps(keyRevs []KeyModRev) ([][]clientv3.Op, int, error) {
	tot := len(keyRevs)
	if tot == 0 {
		return nil, 0, fmt.Errorf("keyRevs is empty")
	}

	// 计算多少组批量请求，每组批量请求不能超过 etcd 限制的事务单次最大请求量 DefaultMaxTxnOpNum
	batchSize := DefaultEtcdMaxTxnOpNum
	groupLen := tot / batchSize
	if tot%batchSize != 0 {
		groupLen++
	}

	// 构造批量 get 请求
	totOps := make([][]clientv3.Op, 0, groupLen)
	ops := make([]clientv3.Op, 0, batchSize)
	cnt := 0
	for _, kr := range keyRevs {
		if cnt == batchSize {
			totOps = append(totOps, ops)
			cnt = 0
			ops = make([]clientv3.Op, 0, batchSize)
		}

		cnt++
		if kr.ModRev > 0 {
			ops = append(ops, clientv3.OpGet(kr.Key, clientv3.WithRev(kr.ModRev)))
		} else {
			ops = append(ops, clientv3.OpGet(kr.Key))
		}
	}
	if len(ops) > 0 {
		totOps = append(totOps, ops)
	}

	return totOps, batchSize, nil
}
