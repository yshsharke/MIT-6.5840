package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	killCh       chan struct{}
	timer        *time.Timer

	// Your definitions here.
	persister      *raft.Persister
	machine        *KVMachine
	notifyChMap    map[int]chan NotifyMsg
	duplicateTable map[int64]DuplicateEntry
	lastApplied    int
	clerk          *shardctrler.Clerk
	config         shardctrler.Config
	reConfigWG     sync.WaitGroup
	shards         [10]ShardStatus
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		reply.Value = ""
		DPrintf(dServer, "G%dS%d %d:%d Key %v Wrong Group\n", kv.gid, kv.me, args.ClientID, args.Seq, args.Key)
		return
	}
	entry, ok := kv.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		reply.Value = entry.Value
		DPrintf(dServer, "G%dS%d Dup %d:%d Get {%v:%v} %v\n", kv.gid, kv.me, args.ClientID, args.Seq, args.Key, reply.Value, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Get", Key: args.Key, Value: ""}
	index, _, isLeader := kv.rf.Start(Command{Type: "Op", Content: op})
	if isLeader {
		DPrintf(dServer, "G%dS%d %d:%d(%d) Get %v\n", kv.gid, kv.me, args.ClientID, args.Seq, index, args.Key)
		ch := kv.getNotifyChannel(index, true)
		defer func() {
			kv.mu.Lock()
			close(ch)
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Value = msg.Value
				reply.Err = msg.Err
			} else {
				reply.Value = ""
				reply.Err = ErrOverWritten
				// DPrintf(dDebug, "G%dS%d %d:%d(%d) Get OverWritten\n", kv.gid, kv.me, args.ClientID, args.Seq, index)
			}
		case <-time.After(RequestTimeout):
			DPrintf(dServer, "G%dS%d %d:%d Get Timeout\n", kv.gid, kv.me, args.ClientID, args.Seq)
			reply.Value = ""
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Value = ""
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		DPrintf(dServer, "G%dS%d %d:%d Key %v Wrong Group\n", kv.gid, kv.me, args.ClientID, args.Seq, args.Key)
		return
	}
	entry, ok := kv.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		DPrintf(dServer, "G%dS%d Dup %d:%d %v {%v:%v} %v\n", kv.gid, kv.me, args.ClientID, args.Seq, args.Op, args.Key, args.Value, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: args.Op, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(Command{Type: "Op", Content: op})
	if isLeader {
		DPrintf(dServer, "G%dS%d %d:%d(%d) %v {%v:%v}\n", kv.gid, kv.me, args.ClientID, args.Seq, index, args.Op, args.Key, args.Value)
		ch := kv.getNotifyChannel(index, true)
		defer func() {
			kv.mu.Lock()
			close(ch)
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Err = msg.Err
			} else {
				reply.Err = ErrOverWritten
				// DPrintf(dDebug, "G%dS%d %d:%d(%d) %v OverWritten\n", kv.gid, kv.me, args.ClientID, args.Seq, index, args.Op)
			}
		case <-time.After(500 * time.Millisecond):
			DPrintf(dServer, "G%dS%d %d:%d %v Timeout\n", kv.gid, kv.me, args.ClientID, args.Seq, args.Op)
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	DPrintf(dConfig, "G%dS%d Mig %d\n", kv.gid, kv.me, args.Shard)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf(dConfig, "G%dS%d Mig ErrLeader\n", kv.gid, kv.me)
		return
	}
	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum-1 || (kv.config.Num == args.ConfigNum-1 && kv.shards[args.Shard] == Available) {
		reply.Err = ErrConfigNum
		kv.mu.Unlock()
		DPrintf(dConfig, "G%dS%d Mig ErrNum %d\n", kv.gid, kv.me, args.ConfigNum)
		return
	}

	data := kv.machine.GetShard(args.Shard)
	newData := make(map[string]string)
	for key, value := range data {
		newData[key] = value
	}

	newDuplicateTable := make(map[int64]DuplicateEntry)
	for key, value := range kv.duplicateTable {
		newDuplicateTable[key] = value
	}
	kv.mu.Unlock()

	reply.Data = newData
	reply.DuplicateTable = newDuplicateTable
	reply.Err = OK
	//DPrintf(dDebug, "G%dS%d reply %v\n", kv.gid, kv.me, reply.Data)
}

func (kv *ShardKV) AckMigrate(args *AckMigrateArgs, reply *AckMigrateReply) {
	DPrintf(dConfig, "G%dS%d AckMig %d\n", kv.gid, kv.me, args.Shard)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num >= args.ConfigNum-1 {
		reply.Err = OK
		go kv.gcShard(args.Shard)
	} else {
		reply.Err = ErrConfigNum
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) poller() {
	for {
		select {
		case <-kv.timer.C:
		case <-kv.killCh:
			return
		}
		kv.timer.Reset(time.Duration(30) * time.Millisecond)
		//kv.timer.Reset(time.Duration(100) * time.Millisecond)

		// Only leader can poll for latest configuration.
		_, isLeader := kv.rf.GetState()
		if isLeader {
			config := kv.clerk.Query(kv.config.Num + 1)
			if config.Num == 0 {
				continue
			}
			kv.mu.Lock()
			if config.Num == kv.config.Num+1 {
				DPrintf(dConfig, "G%dS%d Recfg %d\n", kv.gid, kv.me, config.Num)
				for shard, gid := range kv.config.Shards {
					if gid == kv.gid && config.Shards[shard] != kv.gid {
						// Lose a shard.
						//DPrintf(dDebug, "G%dS%d Lose %d\n", kv.gid, kv.me, shard)
						kv.reConfigWG.Add(1)
						go kv.deleteShard(shard, config.Num)
					}
					if gid != -1 && gid != kv.gid && config.Shards[shard] == kv.gid {
						// Gain a shard.
						//DPrintf(dDebug, "G%dS%d Gain %d\n", kv.gid, kv.me, shard)
						kv.reConfigWG.Add(1)
						go kv.addShard(shard, config.Num, gid)
					}
				}
				kv.mu.Unlock()
				kv.reConfigWG.Wait()

				kv.ackConfig(config.Num, config)
			} else {
				kv.mu.Unlock()
			}
		}

		// time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		kv.rf.Start(Command{Type: "Cfg", Content: Cfg{CfgType: "Nop"}})

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) gcShard(shard int) {
	for !kv.killed() {
		if kv.shards[shard] != Unavailable {
			return
		}

		_, _, isLeader := kv.rf.Start(Command{Type: "Cfg", Content: Cfg{CfgType: "GC", Shard: shard}})
		if !isLeader {
			return
		}

		time.Sleep(time.Duration(40) * time.Millisecond)
	}
}

func (kv *ShardKV) deleteShard(shard int, configNum int) {
	defer kv.reConfigWG.Done()

	for !kv.killed() {
		if kv.config.Num >= configNum || kv.shards[shard] != Available {
			return
		}

		_, _, isLeader := kv.rf.Start(Command{Type: "Cfg", Content: Cfg{CfgType: "Del", Num: configNum, Shard: shard}})
		if !isLeader {
			return
		}

		time.Sleep(time.Duration(40) * time.Millisecond)
	}
}

func (kv *ShardKV) addShard(shard int, configNum int, source int) {
	defer kv.reConfigWG.Done()

	// Pull shard data.
	var data map[string]string
	var duplicateTable map[int64]DuplicateEntry
	args := MigrateArgs{Shard: shard, ConfigNum: configNum}
	for !kv.killed() {
		if data != nil || duplicateTable != nil {
			break
		}
		if servers, ok := kv.config.Groups[source]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply MigrateReply
				DPrintf(dConfig, "G%dS%d Pull %d From %d\n", kv.gid, kv.me, shard, source)
				ok := srv.Call("ShardKV.Migrate", &args, &reply)
				if ok && (reply.Err == OK) {
					DPrintf(dConfig, "G%dS%d Pull %d Suc\n", kv.gid, kv.me, shard)
					data = reply.Data
					duplicateTable = reply.DuplicateTable
					break
				} else if ok && (reply.Err == ErrConfigNum) {
					// Wait for source to reconfig.
					si--
					time.Sleep(time.Duration(20) * time.Millisecond)
				}
				// time.Sleep(time.Duration(50) * time.Millisecond)
			}
		}
	}

	for !kv.killed() {
		if kv.config.Num >= configNum || kv.shards[shard] == Available {
			return
		}

		_, _, isLeader := kv.rf.Start(Command{Type: "Cfg", Content: Cfg{CfgType: "Add", Num: configNum, Shard: shard, Data: data, DuplicateTable: duplicateTable}})
		if !isLeader {
			return
		}

		time.Sleep(time.Duration(40) * time.Millisecond)
	}
}

func (kv *ShardKV) ackConfig(configNum int, config shardctrler.Config) {
	for !kv.killed() {
		if kv.config.Num >= configNum {
			return
		}

		_, _, isLeader := kv.rf.Start(Command{Type: "Cfg", Content: Cfg{CfgType: "Ack", Num: configNum, Config: config}})
		if !isLeader {
			return
		}

		time.Sleep(time.Duration(40) * time.Millisecond)
	}
}

func (kv *ShardKV) migrated(shard int, configNum int, source int) {
	args := AckMigrateArgs{Shard: shard, ConfigNum: configNum}
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			return
		}

		if servers, ok := kv.config.Groups[source]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply AckMigrateReply
				ok := srv.Call("ShardKV.AckMigrate", &args, &reply)
				if ok && (reply.Err == OK) {
					return
				} else if ok && (reply.Err == ErrConfigNum) {
					// Wait for source to reconfig.
					si--
					time.Sleep(time.Duration(40) * time.Millisecond)
				}
			}
		}
	}
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {

		if msg.CommandValid {
			command := msg.Command.(Command)
			// Guarantee apply order.
			if msg.CommandIndex <= kv.lastApplied {
				continue
			} else if msg.CommandIndex == kv.lastApplied+1 {
				kv.lastApplied = msg.CommandIndex
			} else {
				continue
			}

			if command.Type == "Op" {
				op := command.Content.(Op)

				// Check shard again.
				if !kv.checkShard(op.Key) {
					_, isLeader := kv.rf.GetState()
					if isLeader {
						// The notify channel may be closed.
						func() {
							defer func() {
								if r := recover(); r != nil {
									DPrintf(dDebug, "G%dS%d Ch%d closed\n", kv.gid, kv.me, msg.CommandIndex)
								}
							}()
							if ch := kv.getNotifyChannel(msg.CommandIndex, false); ch != nil {
								ch <- NotifyMsg{Value: "", Err: ErrWrongGroup, ClientID: op.ClientID, Seq: op.Seq}
							}
						}()
					}
					continue
				}

				// Return for a duplicated request.
				entry, ok := kv.getDuplicateEntry(op.ClientID)
				if ok && entry.Seq >= op.Seq {
					_, isLeader := kv.rf.GetState()
					if isLeader {
						// The notify channel may be closed.
						func() {
							defer func() {
								if r := recover(); r != nil {
									DPrintf(dDebug, "G%dS%d Ch%d closed\n", kv.gid, kv.me, msg.CommandIndex)
								}
							}()
							if ch := kv.getNotifyChannel(msg.CommandIndex, false); ch != nil {
								ch <- NotifyMsg{Value: entry.Value, Err: entry.Err, ClientID: op.ClientID, Seq: op.Seq}
							}
						}()
					}
					continue
				}

				// Apply to state machine.
				var value = ""
				var err Err = OK
				if op.OpType == "Get" {
					value, err = kv.machine.Get(op.Key)
					DPrintf(dApplier, "G%dA%d %d:%d(%d) Get {%v:%v} %v\n", kv.gid, kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key, value, err)
				} else if op.OpType == "Put" {
					value, err = kv.machine.Put(op.Key, op.Value)
					DPrintf(dApplier, "G%dA%d %d:%d(%d) Put {%v:%v} %v\n", kv.gid, kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key, op.Value, err)
				} else if op.OpType == "Append" {
					value, err = kv.machine.Append(op.Key, op.Value)
					DPrintf(dApplier, "G%dA%d %d:%d(%d) Append {%v:%v} %v\n", kv.gid, kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key, op.Value, err)
				} else {
					DPrintf(dApplier, "G%dA%d %d:%d(%d) Invalid OP %v\n", kv.gid, kv.me, op.ClientID, op.Seq, msg.CommandIndex, op)
					err = ErrInvalidOp
				}

				// Update duplicate entry for future duplicated request.
				kv.mu.Lock()
				kv.duplicateTable[op.ClientID] = DuplicateEntry{Seq: op.Seq, Value: value, Err: err}
				kv.mu.Unlock()

				// Only leader can notify an RPC Handler.
				_, isLeader := kv.rf.GetState()
				if isLeader {
					// The notify channel may be closed.
					func() {
						defer func() {
							if r := recover(); r != nil {
								DPrintf(dDebug, "G%dS%d Ch%d closed\n", kv.gid, kv.me, msg.CommandIndex)
							}
						}()
						if ch := kv.getNotifyChannel(msg.CommandIndex, false); ch != nil {
							ch <- NotifyMsg{Value: value, Err: err, ClientID: op.ClientID, Seq: op.Seq}
						}
					}()
				}
			} else if command.Type == "Cfg" {
				// Reconfig.
				cfg := command.Content.(Cfg)

				// Check config num.
				if cfg.Num != kv.config.Num+1 && cfg.CfgType != "GC" {
					continue
				}

				if cfg.CfgType == "Del" {
					if kv.config.Shards[cfg.Shard] == kv.gid && kv.shards[cfg.Shard] == Available {
						kv.mu.Lock()
						kv.shards[cfg.Shard] = Unavailable
						kv.config.Shards[cfg.Shard] = -1
						kv.mu.Unlock()
						DPrintf(dConfig, "G%dA%d Del %d\n", kv.gid, kv.me, cfg.Shard)
					}
				} else if cfg.CfgType == "Add" {
					if kv.shards[cfg.Shard] != Available {
						newData := make(map[string]string)
						for key, value := range cfg.Data {
							newData[key] = value
						}

						kv.mu.Lock()
						kv.machine.SetShard(cfg.Shard, newData)
						kv.shards[cfg.Shard] = Available
						source := kv.config.Shards[cfg.Shard]
						kv.config.Shards[cfg.Shard] = kv.gid
						for key, value := range cfg.DuplicateTable {
							if _, ok := kv.duplicateTable[key]; !ok {
								kv.duplicateTable[key] = value
							} else if kv.duplicateTable[key].Seq < value.Seq {
								kv.duplicateTable[key] = value
							}
						}
						kv.mu.Unlock()

						go kv.migrated(cfg.Shard, cfg.Num, source)
						DPrintf(dConfig, "G%dA%d Add %d\n", kv.gid, kv.me, cfg.Shard)
					}
				} else if cfg.CfgType == "Ack" {
					// kv.config = cfg.Config
					updateCfg := true
					for shard, status := range kv.shards {
						if status == Unknown {
							continue
						}
						if status != Available && cfg.Config.Shards[shard] == kv.gid {
							updateCfg = false
							break
						}
						if status == Available && cfg.Config.Shards[shard] != kv.gid {
							updateCfg = false
							break
						}
					}
					if updateCfg {
						kv.mu.Lock()
						kv.config = cfg.Config
						for shard, gid := range cfg.Config.Shards {
							if gid == kv.gid {
								kv.shards[shard] = Available
							} else {
								kv.shards[shard] = Unavailable
							}
						}
						kv.mu.Unlock()

						DPrintf(dConfig, "G%dA%d Ack %d\n", kv.gid, kv.me, cfg.Num)
					}
				} else if cfg.CfgType == "GC" {
					if kv.shards[cfg.Shard] == Unavailable {
						kv.mu.Lock()
						kv.shards[cfg.Shard] = Removed
						kv.mu.Unlock()
						kv.machine.DeleteShard(cfg.Shard)

						DPrintf(dConfig, "G%dA%d GC %d\n", kv.gid, kv.me, cfg.Shard)
					}
				}
			}

			// Create Snapshot if needed.
			if kv.maxraftstate != -1 && kv.maxraftstate*2 < kv.persister.RaftStateSize() {
				// DPrintf(dDebug, "G%dS%d State %d > %d", kv.gid, kv.me, kv.persister.RaftStateSize(), 4*kv.maxraftstate)
				kv.rf.Snapshot(kv.lastApplied, kv.createSnapshot())
			}
		} else if msg.SnapshotValid {
			//if msg.SnapshotIndex < kv.lastApplied {
			//	panic("error: move back")
			//}
			DPrintf(dSnapshot, "G%dS%d Install Snap %d\n", kv.gid, kv.me, msg.SnapshotIndex)
			kv.installSnapshot(msg.Snapshot)
			kv.rf.SyncSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		}
	}
}

func (kv *ShardKV) getDuplicateEntry(clientID int64) (DuplicateEntry, bool) {
	if clientID == 0 {
		return DuplicateEntry{Seq: -1}, true
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.duplicateTable[clientID]
	return entry, ok
}

func (kv *ShardKV) getNotifyChannel(index int, create bool) chan NotifyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := kv.notifyChMap[index]
	if create {
		kv.notifyChMap[index] = make(chan NotifyMsg)
		ch = kv.notifyChMap[index]
	}
	return ch
}

func (kv *ShardKV) checkShard(key string) bool {
	if key == "" {
		return true
	}
	shard := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shards[shard] == Available
}

func (kv *ShardKV) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.lastApplied)
	e.Encode(kv.duplicateTable)
	e.Encode(kv.machine.GetState())
	e.Encode(kv.config)
	e.Encode(kv.shards)
	kv.mu.Unlock()
	data := w.Bytes()
	// DPrintf(dSnapshot, "G%dS%d Create\n", kv.gid, kv.me)
	return data
}

func (kv *ShardKV) installSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var duplicateTable map[int64]DuplicateEntry
	var state map[int]map[string]string
	var config shardctrler.Config
	var shards [10]ShardStatus
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&duplicateTable) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&shards) != nil {

	} else {
		kv.mu.Lock()
		kv.lastApplied = lastApplied
		kv.duplicateTable = duplicateTable
		kv.config = config
		kv.shards = shards
		DPrintf(dSnapshot, "G%dS%d Install Shards %v", kv.gid, kv.me, shards)
		kv.machine.SetState(state)
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.timer.Stop()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	DPrintf(dKill, "G%dS%d Killed\n", kv.gid, kv.me)
	go func() {
		time.Sleep(time.Duration(5) * time.Second)
		close(kv.applyCh)
		kv.killCh <- struct{}{}
	}()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(Op{})
	labgob.Register(Cfg{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.timer = time.NewTimer(time.Duration(10) * time.Millisecond)

	// Your initialization code here.
	kv.persister = persister
	kv.machine = MakeMachine(gid, me)
	kv.notifyChMap = make(map[int]chan NotifyMsg)
	kv.duplicateTable = make(map[int64]DuplicateEntry)
	kv.clerk = shardctrler.MakeClerk(ctrlers)
	kv.config = shardctrler.Config{Num: 0}
	for shard := range kv.config.Shards {
		kv.config.Shards[shard] = -1
	}
	for shard := range kv.shards {
		kv.shards[shard] = Unknown
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.killCh = make(chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.installSnapshot(kv.persister.ReadSnapshot())

	DPrintf(dServer, "G%dS%d Start Max %d Config %v\n", kv.gid, kv.me, kv.maxraftstate, kv.config)
	//for shard, gid := range kv.config.Shards {
	//	if gid == kv.gid {
	//		kv.shards[shard] = Available
	//	} else {
	//		kv.shards[shard] = Unavailable
	//	}
	//}

	go kv.applier()

	go kv.poller()

	// go kv.ticker()

	return kv
}
