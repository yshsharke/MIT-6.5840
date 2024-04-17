package shardkv

import (
	"sync"
)

type KVMachine struct {
	gid    int
	me     int
	shards map[int]map[string]string
	mu     sync.Mutex
}

func (kv *KVMachine) Get(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	if kv.shards[shard] == nil {
		kv.shards[shard] = make(map[string]string)
	}
	value, ok := kv.shards[shard][key]
	if ok {
		return value, OK
	} else {
		return "", ErrNoKey
	}
}

func (kv *KVMachine) Put(key string, value string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	if kv.shards[shard] == nil {
		kv.shards[shard] = make(map[string]string)
	}
	kv.shards[shard][key] = value
	return "", OK
}

func (kv *KVMachine) Append(key string, value string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	if kv.shards[shard] == nil {
		kv.shards[shard] = make(map[string]string)
	}
	kv.shards[shard][key] += value
	return "", OK
}

func (kv *KVMachine) GetShard(shard int) map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.shards[shard] == nil {
		kv.shards[shard] = make(map[string]string)
	}
	return kv.shards[shard]
}

func (kv *KVMachine) SetShard(shard int, data map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.shards[shard] = data
}

func (kv *KVMachine) DeleteShard(shard int) {
	kv.SetShard(shard, nil)
}

func (kv *KVMachine) SetState(state map[int]map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.shards = state
}

func (kv *KVMachine) GetState() map[int]map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shards
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(kv.state)
	//snapshot := w.Bytes()
	//return snapshot
}

func MakeMachine(gid int, me int) *KVMachine {
	kv := new(KVMachine)
	kv.gid = gid
	kv.me = me
	kv.shards = make(map[int]map[string]string)

	return kv
}
