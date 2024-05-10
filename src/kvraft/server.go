package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister      *raft.Persister
	machine        *KVMachine
	notifyChMap    map[int]chan NotifyMsg
	duplicateTable map[int64]DuplicateEntry

	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry, ok := kv.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		reply.Value = entry.Value
		DPrintf(dServer, "S%d Dup %d:%d Get {%v:%v} %v\n", kv.me, args.ClientID, args.Seq, args.Key, reply.Value, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Get", Key: args.Key, Value: ""}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		DPrintf(dServer, "S%d %d:%d(%d) Get %v\n", kv.me, args.ClientID, args.Seq, index, args.Key)
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
			}
		case <-time.After(RequestTimeout):
			DPrintf(dServer, "S%d %d:%d Get Timeout\n", kv.me, args.ClientID, args.Seq)
			reply.Value = ""
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Value = ""
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry, ok := kv.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		DPrintf(dServer, "S%d Dup %d:%d %v {%v:%v} %v\n", kv.me, args.ClientID, args.Seq, args.Op, args.Key, args.Value, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: args.Op, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		DPrintf(dServer, "S%d %d:%d(%d) %v {%v:%v}\n", kv.me, args.ClientID, args.Seq, index, args.Op, args.Key, args.Value)
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
			}
		case <-time.After(500 * time.Millisecond):
			DPrintf(dServer, "S%d %d:%d %v Timeout\n", kv.me, args.ClientID, args.Seq, args.Op)
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			// Return for a duplicated request.
			if msg.CommandIndex <= kv.lastApplied {
				continue
			} else if msg.CommandIndex == kv.lastApplied+1 {
				kv.lastApplied = msg.CommandIndex
			} else {
				continue
			}

			entry, ok := kv.getDuplicateEntry(op.ClientID)
			if ok && entry.Seq >= op.Seq {
				_, isLeader := kv.rf.GetState()
				if isLeader {
					// The notify channel may be closed.
					func() {
						defer func() {
							if r := recover(); r != nil {
								// DPrintf(dDebug, "S%d Ch%d closed\n", kv.me, msg.CommandIndex)
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
				DPrintf(dApplier, "A%d %d:%d(%d) Get %v\n", kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key)
				value, err = kv.machine.Get(op.Key)
			} else if op.OpType == "Put" {
				DPrintf(dApplier, "A%d %d:%d(%d) Put {%v:%v}\n", kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key, op.Value)
				value, err = kv.machine.Put(op.Key, op.Value)
			} else if op.OpType == "Append" {
				DPrintf(dApplier, "A%d %d:%d(%d) Append {%v:%v}\n", kv.me, op.ClientID, op.Seq, msg.CommandIndex, op.Key, op.Value)
				value, err = kv.machine.Append(op.Key, op.Value)
			} else {
				DPrintf(dApplier, "A%d %d:%d(%d) Invalid OP %v\n", kv.me, op.ClientID, op.Seq, msg.CommandIndex, op)
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
							// DPrintf(dDebug, "S%d Ch%d closed\n", kv.me, msg.CommandIndex)
						}
					}()
					if ch := kv.getNotifyChannel(msg.CommandIndex, false); ch != nil {
						ch <- NotifyMsg{Value: value, Err: err, ClientID: op.ClientID, Seq: op.Seq}
					}
				}()
			}

			// Create Snapshot.
			if kv.maxraftstate != -1 && kv.maxraftstate*4 < kv.persister.RaftStateSize() {
				// DPrintf(dDebug, "S%d State %d > %d", kv.me, kv.persister.RaftStateSize(), 4*kv.maxraftstate)
				kv.rf.Snapshot(kv.lastApplied, kv.createSnapshot())
			}
		} else if msg.SnapshotValid {
			//if msg.SnapshotIndex < kv.lastApplied {
			//	fmt.Printf("Snap %d < Last %d\n", msg.SnapshotIndex, kv.lastApplied)
			//	panic("error: move back")
			//}
			DPrintf(dSnapshot, "S%d Install Snap %d\n", kv.me, msg.SnapshotIndex)
			kv.installSnapshot(msg.Snapshot)
			kv.rf.SyncSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		}
	}
}

func (kv *KVServer) getDuplicateEntry(clientID int64) (DuplicateEntry, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.duplicateTable[clientID]
	return entry, ok
}

func (kv *KVServer) getNotifyChannel(index int, create bool) chan NotifyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := kv.notifyChMap[index]
	if create {
		kv.notifyChMap[index] = make(chan NotifyMsg)
		ch = kv.notifyChMap[index]
	}
	return ch
}

func (kv *KVServer) createSnapshot() []byte {
	//DPrintf(dDebug, "S%d Table %v\n", kv.me, kv.duplicateTable)
	//DPrintf(dDebug, "S%d State %v\n", kv.me, kv.machine.GetState())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// kv.mu.Lock()
	e.Encode(kv.lastApplied)
	e.Encode(kv.duplicateTable)
	e.Encode(kv.machine.GetState())
	// kv.mu.Unlock()
	data := w.Bytes()
	DPrintf(dSnapshot, "S%d Create\n", kv.me)
	return data
}

func (kv *KVServer) installSnapshot(data []byte) {
	DPrintf(dSnapshot, "S%d Install\n", kv.me)
	//DPrintf(dDebug, "S%d Table %v\n", kv.me, kv.duplicateTable)
	//DPrintf(dDebug, "S%d State %v\n", kv.me, kv.machine.GetState())
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var duplicateTable map[int64]DuplicateEntry
	var state map[string]string
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&duplicateTable) != nil ||
		d.Decode(&state) != nil {

	} else {
		// kv.mu.Lock()
		kv.lastApplied = lastApplied
		kv.duplicateTable = duplicateTable
		kv.machine.SetState(state)
		// kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.machine = MakeMachine(me)
	kv.notifyChMap = make(map[int]chan NotifyMsg)
	kv.duplicateTable = make(map[int64]DuplicateEntry)

	DPrintf(dServer, "S%d Start Max %d\n", kv.me, kv.maxraftstate)

	kv.installSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()

	return kv
}
