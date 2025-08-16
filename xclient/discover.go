package xclient

import (
	"errors"
	"math/rand/v2"
	"sync"
)

type selectMode int // 选择模式

const (
	RandomSelect     selectMode = iota // 随机选择
	RoundRobinSelect                   // 轮询选择
)

// Discovery 是一个接口类型，包含了服务发现所需要的最基本的接口
type Discovery interface {
	Refresh() error                      // 刷新服务列表
	Update(servers []string) error       // 更新服务列表
	Get(mode selectMode) (string, error) // 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)           // 获取所有服务
}

// 不需要注册中心，服务列表由手工维护的服务发现的结构体
type MultiServerDiscovery struct {
	r       *rand.Rand // 随机数生成器
	mu      sync.Mutex // 互斥锁
	servers []string
	index   int // index 记录 Round Robin 算法已经轮询到的位置，为了避免每次从 0 开始，初始化时随机设定一个值。
}

func NewMultiServerDiscovery(servers []string) *MultiServerDiscovery {
	return &MultiServerDiscovery{
		r:       rand.New(rand.NewPCG(0, 0)),
		servers: servers,
		index:   rand.IntN(len(servers)),
	}
}


var _ Discovery = (*MultiServerDiscovery)(nil) // 结构实现检查

// 无实际意义，只是为了实现Discovery接口 因为是手工维护
func (d *MultiServerDiscovery) Refresh() error {
	return nil
}

func (d *MultiServerDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

func (d *MultiServerDiscovery) Get(mode selectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no server")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.IntN(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not support select mode")
	}
}

func (d *MultiServerDiscovery) GetAll() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.servers, nil
}
