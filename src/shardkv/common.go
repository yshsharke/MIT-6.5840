package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInvalidOp   = "ErrInvalidOp"
	ErrOverWritten = "ErrOverWritten"
	ErrTimeout     = "ErrTimeout"
	ErrConfigNum   = "ErrConfigNum"
)

const RequestTimeout = 500 * time.Millisecond

type Err string

type Command struct {
	Type    string
	Content interface{}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int64
	OpType   string
	Key      string
	Value    string
}

type Cfg struct {
	CfgType        string
	Num            int
	Shard          int
	Data           map[string]string
	Config         shardctrler.Config
	DuplicateTable map[int64]DuplicateEntry
}

type NotifyMsg struct {
	Err      Err
	Value    string
	ClientID int64
	Seq      int64
}

type DuplicateEntry struct {
	Seq   int64
	Err   Err
	Value string
}

type ShardStatus string

const (
	Unavailable ShardStatus = "Unavailable"
	Available   ShardStatus = "Available"
	Pulling     ShardStatus = "Pulling"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientID int64
	Seq      int64
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err   Err
	Value string // Always be ""
}

type GetArgs struct {
	ClientID int64
	Seq      int64
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err            Err
	Data           map[string]string
	DuplicateTable map[int64]DuplicateEntry
}

const Debug = true

type logTopic string

const (
	dClient   logTopic = "CLNT"
	dServer   logTopic = "SEVR"
	dMachine  logTopic = "MACH"
	dApplier  logTopic = "APPL"
	dSnapshot logTopic = "SNAP"
	dKill     logTopic = "KILL"
	dTest     logTopic = "TEST"
	dDebug    logTopic = "DBUG"
	dConfig   logTopic = "DCFG"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		microseconds := time.Since(debugStart).Microseconds()
		microseconds /= 100
		prefix := fmt.Sprintf("%06d %v ", microseconds, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
