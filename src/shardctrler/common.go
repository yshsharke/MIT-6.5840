package shardctrler

import (
	"fmt"
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInvalidOp   = "ErrInvalidOp"
	ErrOverWritten = "ErrOverWritten"
	ErrTimeout     = "ErrTimeout"
	ErrKilled      = "ErrKilled"
)

type Err string

type JoinArgs struct {
	ClientID int64
	Seq      int64
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClientID int64
	Seq      int64
	GIDs     []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClientID int64
	Seq      int64
	Shard    int
	GID      int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ClientID int64
	Seq      int64
	Num      int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}

type NotifyMsg struct {
	Err      Err
	Config   Config
	ClientID int64
	Seq      int64
}

type DuplicateEntry struct {
	Seq    int64
	Err    Err
	Config Config
}

const RequestTimeout = 500 * time.Millisecond

const Debug = false

type logTopic string

const (
	dTest    logTopic = "TEST"
	dDebug   logTopic = "DBUG"
	dCtrler  logTopic = "CTRL"
	dApplier logTopic = "APPL"
	dMachine logTopic = "MACH"
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
