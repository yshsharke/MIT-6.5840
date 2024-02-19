package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type ArgType int

const (
	Schedule ArgType = iota
	Report
)

type ReplyType int

const (
	Wait ReplyType = iota
	Run
	Exit
)

type RequestArgs struct {
	Type     ArgType
	Number   int
	WorkType WorkType
}

type RequestReply struct {
	Type     ReplyType
	Number   int
	WorkType WorkType
	Filename string
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
