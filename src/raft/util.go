package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = false

type logTopic string

const (
	dTest     logTopic = "TEST"
	dTimer    logTopic = "TIMR"
	dVote     logTopic = "VOTE"
	dLeader   logTopic = "LEAD"
	dFollower logTopic = "FOLL"
	dTerm     logTopic = "TERM"
	dAppend   logTopic = "APPD"
	dKill     logTopic = "KILL"
	dApply    logTopic = "APPL"
	dPersist  logTopic = "PSST"
	dSnapshot logTopic = "SNAP"
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
