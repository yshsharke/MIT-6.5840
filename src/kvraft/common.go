package kvraft

import (
	"fmt"
	"log"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInvalidOp   = "ErrInvalidOp"
	ErrOverWritten = "ErrOverWritten"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const RequestTimeout = 500 * time.Millisecond

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

// Put or Append
type PutAppendArgs struct {
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
}

type GetReply struct {
	Err   Err
	Value string
}

const Debug = true

type logTopic string

const (
	dClient   logTopic = "CLNT"
	dServer   logTopic = "SEVR"
	dMachine  logTopic = "MACH"
	dApplier  logTopic = "APPL"
	dSnapshot logTopic = "SNAP"
	dTest     logTopic = "TEST"
	dDebug    logTopic = "DBUG"
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
