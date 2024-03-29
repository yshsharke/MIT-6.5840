package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientID = nrand() % 1000000
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seq++
	args := &GetArgs{ClientID: ck.clientID, Seq: ck.seq, Key: key}
	reply := &GetReply{}
	for {
		ck.leader %= len(ck.servers)
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			reply = &GetReply{}
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			reply = &GetReply{}
			continue
		} else if reply.Err == OK {
			return reply.Value
		} else {
			return ""
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := &PutAppendArgs{ClientID: ck.clientID, Seq: ck.seq, Key: key, Value: value, Op: op}
	reply := &PutAppendReply{}
	for {
		ck.leader %= len(ck.servers)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			reply = &PutAppendReply{}
			ck.leader++
			continue
		} else if reply.Err == ErrOverWritten {
			reply = &PutAppendReply{}
			continue
		} else {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
