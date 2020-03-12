package etcd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"sync"
	"time"
)

type DisClient struct {
	client       *clientv3.Client
	serverList   map[string]string
	lock         sync.Mutex
	onNewService func(m map[string]string)
	onOffService func(m map[string]string)
}

func NewDisClient(addr []string, onNewService, onOffService func(m map[string]string)) (*DisClient, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	if client, err := clientv3.New(conf); err == nil {
		return &DisClient{
			client:       client,
			serverList:   make(map[string]string, 8),
			onNewService: onNewService,
			onOffService: onOffService,
		}, nil
	} else {
		return nil, err
	}
}

func (d *DisClient) Discover(prefix string) (map[string]string, error) {
	resp, err := d.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := d.extractAddrs(resp)
	go d.watcher(prefix)
	return addrs, nil
}

func (d *DisClient) watcher(prefix string) {
	rch := d.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //put
				d.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
				if d.onNewService != nil {
					go d.onNewService(d.CurentList()) //执行新增服务逻辑
				}
			case mvccpb.DELETE: //delete
				d.DelServiceList(string(ev.Kv.Key))
				if d.onOffService != nil {
					go d.onOffService(d.CurentList()) //执行删除服务逻辑
				}
			}
		}
	}
}

func (d *DisClient) extractAddrs(resp *clientv3.GetResponse) map[string]string {
	addrs := make(map[string]string, 8)
	if resp == nil || resp.Kvs == nil {
		return addrs
	}
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			d.SetServiceList(string(resp.Kvs[i].Key), string(v))
			addrs[string(resp.Kvs[i].Key)] = string(v)
		}
	}
	return addrs
}

func (d *DisClient) SetServiceList(key, val string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.serverList[key] = val
}

func (d *DisClient) DelServiceList(key string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.serverList, key)
}

func (d *DisClient) CurentList() map[string]string {
	d.lock.Lock()
	defer d.lock.Unlock()

	addrs := make(map[string]string, 8)
	for k, v := range d.serverList {
		addrs[k] = v
	}
	return addrs
}

func (d *DisClient) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
