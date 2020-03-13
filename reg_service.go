package etcd

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"time"
)

//创建租约注册服务
type RegService struct {
	client        *clientv3.Client
	lease         clientv3.Lease
	leaseResp     *clientv3.LeaseGrantResponse
	cancelFunc    func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string
	onLeaseOff    func()
}

func NewRegService(addr []string, ttl int64, onLeaseOff func()) (*RegService, error) {
	var conf = clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	var client *clientv3.Client
	var err error

	if client, err = clientv3.New(conf); err != nil {
		return nil, err
	}

	rs := &RegService{
		client:     client,
		onLeaseOff: onLeaseOff,
	}

	if err := rs.setLease(ttl); err != nil {
		return nil, err
	}

	go rs.listenLeaseRespChan()
	return rs, nil
}

//设置租约
func (r *RegService) setLease(ttl int64) error {
	lease := clientv3.NewLease(r.client)

	//设置租约时间
	leaseResp, err := lease.Grant(context.Background(), ttl)
	if err != nil {
		return err
	}

	//设置续租
	ctx, cancelFunc := context.WithCancel(context.Background())
	leaseRespChan, err := lease.KeepAlive(ctx, leaseResp.ID)

	if err != nil {
		return err
	}

	r.lease = lease
	r.leaseResp = leaseResp
	r.cancelFunc = cancelFunc
	r.keepAliveChan = leaseRespChan
	return nil
}

//监听 续租情况
func (r *RegService) listenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-r.keepAliveChan:
			if leaseKeepResp == nil {
				_ = r.lease.Close()
				if r.onLeaseOff != nil {
					r.onLeaseOff()
				}
				return
			}
		}
	}
}

//通过租约 注册服务
func (r *RegService) PutService(key, val string) error {
	kv := clientv3.NewKV(r.client)
	_, err := kv.Put(context.Background(), key, val,
		clientv3.WithLease(r.leaseResp.ID))
	return err
}

//撤销租约
func (r *RegService) RevokeLease() error {
	r.cancelFunc()
	time.Sleep(2 * time.Second)
	_, err := r.lease.Revoke(context.Background(), r.leaseResp.ID)
	return err
}

func (r *RegService) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
