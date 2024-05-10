package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	killCh  chan struct{}

	// Your data here.
	machine        *SCMachine
	notifyChMap    map[int]chan NotifyMsg
	duplicateTable map[int64]DuplicateEntry

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType   string
	ClientID int64
	Seq      int64

	// Join
	Servers map[int][]string // new GID -> servers mappings

	// Leave
	GIDs []int

	// Move
	Shard int
	GID   int

	// Query
	Num int // desired config number
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrKilled
		return
	}
	entry, ok := sc.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		// Dprintf(dCtrler, "S%d Dup %d:%d Join %v\n", sc.me, args.ClientID, args.Seq, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Join", Servers: args.Servers}
	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		// Dprintf(dCtrler, "S%d %d:%d(%d) Join %v\n", sc.me, args.ClientID, args.Seq, index, args.Servers)
		ch := sc.getNotifyChannel(index, true)
		defer func() {
			sc.mu.Lock()
			close(ch)
			delete(sc.notifyChMap, index)
			sc.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Err = msg.Err
			} else {
				// Dprintf(dCtrler, "S%d %d:%d Join OverWritten\n", sc.me, args.ClientID, args.Seq)
				reply.Err = ErrOverWritten
			}
		case <-time.After(RequestTimeout):
			// Dprintf(dCtrler, "S%d %d:%d Join Timeout\n", sc.me, args.ClientID, args.Seq)
			reply.Err = ErrTimeout
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrKilled
		return
	}
	entry, ok := sc.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		// Dprintf(dCtrler, "S%d Dup %d:%d Leave %v\n", sc.me, args.ClientID, args.Seq, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Leave", GIDs: args.GIDs}
	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		// Dprintf(dCtrler, "S%d %d:%d(%d) Leave %v\n", sc.me, args.ClientID, args.Seq, index, args.GIDs)
		ch := sc.getNotifyChannel(index, true)
		defer func() {
			sc.mu.Lock()
			close(ch)
			delete(sc.notifyChMap, index)
			sc.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Err = msg.Err
			} else {
				// Dprintf(dCtrler, "S%d %d:%d Leave OverWritten\n", sc.me, args.ClientID, args.Seq)
				reply.Err = ErrOverWritten
			}
		case <-time.After(RequestTimeout):
			// Dprintf(dCtrler, "S%d %d:%d Leave Timeout\n", sc.me, args.ClientID, args.Seq)
			reply.Err = ErrTimeout
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrKilled
		return
	}
	entry, ok := sc.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Err = entry.Err
		// Dprintf(dCtrler, "S%d Dup %d:%d Move %v\n", sc.me, args.ClientID, args.Seq, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Move", Shard: args.Shard, GID: args.GID}
	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		// Dprintf(dCtrler, "S%d %d:%d(%d) Move %v %v\n", sc.me, args.ClientID, args.Seq, index, args.Shard, args.GID)
		ch := sc.getNotifyChannel(index, true)
		defer func() {
			sc.mu.Lock()
			close(ch)
			delete(sc.notifyChMap, index)
			sc.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Err = msg.Err
			} else {
				// Dprintf(dCtrler, "S%d %d:%d Move OverWritten\n", sc.me, args.ClientID, args.Seq)
				reply.Err = ErrOverWritten
			}
		case <-time.After(RequestTimeout):
			// Dprintf(dCtrler, "S%d %d:%d Move Timeout\n", sc.me, args.ClientID, args.Seq)
			reply.Err = ErrTimeout
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrKilled
		return
	}
	entry, ok := sc.getDuplicateEntry(args.ClientID)
	if ok && entry.Seq >= args.Seq {
		reply.Config = entry.Config
		reply.Err = entry.Err
		// Dprintf(dCtrler, "S%d Dup %d:%d Query %v %v\n", sc.me, args.ClientID, args.Seq, reply.Config, reply.Err)
		return
	}
	op := Op{ClientID: args.ClientID, Seq: args.Seq, OpType: "Query", Num: args.Num}
	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		// Dprintf(dCtrler, "S%d %d:%d(%d) Query %v\n", sc.me, args.ClientID, args.Seq, index, args.Num)
		ch := sc.getNotifyChannel(index, true)
		defer func() {
			sc.mu.Lock()
			close(ch)
			delete(sc.notifyChMap, index)
			sc.mu.Unlock()
		}()

		// Notified or timeout.
		select {
		case msg := <-ch:
			if msg.ClientID == args.ClientID && msg.Seq == args.Seq {
				reply.Config = msg.Config
				reply.Err = msg.Err
			} else {
				// Dprintf(dCtrler, "S%d %d:%d Query OverWritten\n", sc.me, args.ClientID, args.Seq)
				reply.Err = ErrOverWritten
			}
		case <-time.After(RequestTimeout):
			// Dprintf(dCtrler, "S%d %d:%d Query Timeout\n", sc.me, args.ClientID, args.Seq)
			reply.Err = ErrTimeout
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)

	go func() {
		time.Sleep(time.Duration(5) * time.Second)
		close(sc.applyCh)
		// sc.killCh <- struct{}{}
	}()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) getDuplicateEntry(clientID int64) (DuplicateEntry, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	entry, ok := sc.duplicateTable[clientID]
	return entry, ok
}

func (sc *ShardCtrler) getNotifyChannel(index int, create bool) chan NotifyMsg {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch := sc.notifyChMap[index]
	if create {
		sc.notifyChMap[index] = make(chan NotifyMsg)
		ch = sc.notifyChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {

		if msg.CommandValid {
			op := msg.Command.(Op)
			// Return for a duplicated request.
			entry, ok := sc.getDuplicateEntry(op.ClientID)
			if ok && entry.Seq >= op.Seq {
				_, isLeader := sc.rf.GetState()
				if isLeader {
					// The notify channel may be closed.
					func() {
						defer func() {
							if r := recover(); r != nil {
								// Dprintf(dDebug, "S%d Ch%d closed\n", sc.me, msg.CommandIndex)
							}
						}()
						if ch := sc.getNotifyChannel(msg.CommandIndex, false); ch != nil {
							ch <- NotifyMsg{Config: entry.Config, Err: entry.Err, ClientID: op.ClientID, Seq: op.Seq}
						}
					}()
				}
				continue
			}

			// Apply to state machine.
			var config Config
			var err Err = OK
			if op.OpType == "Join" {
				// Dprintf(dApplier, "A%d %d:%d(%d) Join %v\n", sc.me, op.ClientID, op.Seq, msg.CommandIndex, op.Servers)
				err = sc.machine.Join(op.Servers)
			} else if op.OpType == "Leave" {
				// Dprintf(dApplier, "A%d %d:%d(%d) Leave %v\n", sc.me, op.ClientID, op.Seq, msg.CommandIndex, op.GIDs)
				err = sc.machine.Leave(op.GIDs)
			} else if op.OpType == "Move" {
				// Dprintf(dApplier, "A%d %d:%d(%d) Move %v %v\n", sc.me, op.ClientID, op.Seq, msg.CommandIndex, op.Shard, op.GID)
				err = sc.machine.Move(op.Shard, op.GID)
			} else if op.OpType == "Query" {
				// Dprintf(dApplier, "A%d %d:%d(%d) Query %v\n", sc.me, op.ClientID, op.Seq, msg.CommandIndex, op.Num)
				config, err = sc.machine.Query(op.Num)
			} else {
				// Dprintf(dApplier, "A%d %d:%d(%d) Invalid OP %v\n", sc.me, op.ClientID, op.Seq, msg.CommandIndex, op.OpType)
				err = ErrInvalidOp
			}

			// Update duplicate entry for future duplicated request.
			sc.mu.Lock()
			sc.duplicateTable[op.ClientID] = DuplicateEntry{Seq: op.Seq, Config: config, Err: err}
			sc.mu.Unlock()

			// Only leader can notify an RPC Handler.
			_, isLeader := sc.rf.GetState()
			if isLeader {
				// The notify channel may be closed.
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Dprintf(dDebug, "S%d Ch%d closed\n", sc.me, msg.CommandIndex)
						}
					}()
					if ch := sc.getNotifyChannel(msg.CommandIndex, false); ch != nil {
						ch <- NotifyMsg{Config: config, Err: err, ClientID: op.ClientID, Seq: op.Seq}
					}
				}()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.killCh = make(chan struct{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.machine = MakeMachine(me)
	sc.notifyChMap = make(map[int]chan NotifyMsg)
	sc.duplicateTable = make(map[int64]DuplicateEntry)

	// Dprintf(dTest, "S%d Start\n", sc.me)

	go sc.applier()

	return sc
}
