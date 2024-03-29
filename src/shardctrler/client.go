package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID int64
	seq      int64
	leader   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientID = nrand() % 1000000
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.seq++
	args.Num = num
	args.ClientID = ck.clientID
	args.Seq = ck.seq

	for {
		ck.leader %= len(ck.servers)
		reply := &QueryReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			continue
		} else {
			return reply.Config
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.seq++
	args.Servers = servers
	args.ClientID = ck.clientID
	args.Seq = ck.seq

	for {
		ck.leader %= len(ck.servers)
		reply := &JoinReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			continue
		} else {
			return
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.seq++
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.Seq = ck.seq

	for {
		ck.leader %= len(ck.servers)
		reply := &LeaveReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			continue
		} else {
			return
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.seq++
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.Seq = ck.seq

	for {
		ck.leader %= len(ck.servers)
		reply := &MoveReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			continue
		} else {
			return
		}
	}
}
