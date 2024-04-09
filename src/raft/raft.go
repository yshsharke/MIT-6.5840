package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	killCh         chan struct{}

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	indexOffset int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Cmd  interface{}
	Term int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.indexOffset)

	raftState := w.Bytes()
	rf.persister.Save(raftState, snapshot)
	DPrintf(dPersist, "S%d persist\n", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var indexOffset int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&indexOffset) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.indexOffset = indexOffset
		rf.lastApplied = indexOffset
		rf.commitIndex = indexOffset
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getSliceIndex(index)
	if snapshotIndex < 1 {
		return
	}
	DPrintf(dSnapshot, "S%d snapshot %d\n", rf.me, index)
	// The last snapshot log is copied to log[0], its term maybe useful in the early Lab 2D.
	rf.log = append([]LogEntry{}, rf.log[snapshotIndex:]...)
	// rf.log[0].Term = 0
	rf.log[0].Cmd = nil
	rf.indexOffset = index
	rf.persist(snapshot)
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf(dSnapshot, "S%d <- S%d Snap T%d LLI%d LLT%d\n", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		DPrintf(dTerm, "S%d update T%d to T%d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist(nil)
	}

	// Reply immediately if term < currentTerm.
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf(dTerm, "S%d T%d ignore T%d snapshot\n", rf.me, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	if rf.getLogicalIndex(1) > args.LastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		DPrintf(dSnapshot, "S%d LLI%d ignore LLI%d\n", rf.me, rf.getLogicalIndex(0), args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	snapshotIndex := 1
	for ; snapshotIndex < len(rf.log); snapshotIndex++ {
		if rf.getLogicalIndex(snapshotIndex) == args.LastIncludedIndex && rf.log[snapshotIndex].Term == args.LastIncludedTerm {
			snapshotIndex++
			break
		}
	}
	rf.log = append([]LogEntry{}, rf.log[snapshotIndex-1:]...)
	rf.log[0].Cmd = nil
	rf.log[0].Term = args.LastIncludedTerm // Log[0].Term indicates last includedTerm of the last snapshot.
	rf.indexOffset = args.LastIncludedIndex
	DPrintf(dDebug, "S%d IO to %d\n", rf.me, rf.indexOffset)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist(args.Data)
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	DPrintf(dApply, "S%d apply snap T%d LLI%d LLT%d\n", rf.me, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()

	rf.applyCh <- applyMsg
	reply.Success = true
}

func (rf *Raft) SyncSnapshot(index int, term int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.getLogicalIndex(0) != index {
		return
	} else {
		rf.commitIndex = index
		rf.lastApplied = index
		rf.persist(snapshot)
		return
	}
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf(dVote, "S%d -> S%d not vote: T%d < T%d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.persist(nil)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.getLogicalIndex(len(rf.log)-1)) {
			rf.resetElectionTimer()
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.currentTerm = args.Term
			DPrintf(dVote, "S%d -> S%d vote\n", rf.me, args.CandidateId)
			rf.persist(nil)
			return
		}
		DPrintf(dVote, "S%d -> S%d not vote: LLT %d > %d | LLI %d > %d \n", rf.me, args.CandidateId, rf.log[len(rf.log)-1].Term, args.LastLogTerm, rf.getLogicalIndex(len(rf.log)-1), args.LastLogIndex)
	} else {
		DPrintf(dVote, "S%d -> S%d not vote: vote S%d\n", rf.me, args.CandidateId, rf.votedFor)
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// For optimization of nextIndex back up.
	Xterm  int
	XIndex int
	Xlen   int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(dFollower, "S%d <- S%d T%d PLI%d PLT%d LC%d %v\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm. (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf(dTerm, "S%d T%d ignore T%d append\n", rf.me, rf.currentTerm, args.Term)
		return
	}

	rf.resetElectionTimer()

	// While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader.
	if args.Term > rf.currentTerm || rf.state != Follower {
		DPrintf(dTerm, "S%d update T%d to T%d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist(nil)
	}

	if rf.getLogicalIndex(0) > args.PrevLogIndex {
		reply.Success = true
		return
	}

	// Reply false if log does not contain an entry at prevLogIndex
	// whose term matches prevLogTerm. (§5.3)
	// Implement optimization of nextIndex back up.
	if rf.getLogicalIndex(len(rf.log)-1) < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Xterm = -1
		reply.XIndex = -1
		reply.Xlen = rf.getLogicalIndex(len(rf.log) - 1)
		DPrintf(dFollower, "S%d LI%d < PLI%d Xlen%d", rf.me, rf.getLogicalIndex(len(rf.log)-1), args.PrevLogIndex, reply.Xlen)
		return
	}

	if rf.log[rf.getSliceIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Xterm = rf.log[rf.getSliceIndex(args.PrevLogIndex)].Term
		i := rf.getSliceIndex(args.PrevLogIndex) - 1
		for ; i > 0; i-- {
			if rf.log[i].Term != reply.Xterm {
				i++
				break
			}
		}
		reply.XIndex = rf.getLogicalIndex(i)
		reply.Xlen = rf.getLogicalIndex(len(rf.log) - 1)
		DPrintf(dFollower, "S%d no T%d at %d XTerm%d XIndex%d XLen%d\n", rf.me, args.PrevLogTerm, args.PrevLogIndex, reply.Xterm, reply.XIndex, reply.Xlen)
		return
	}

	// If an existing entry conflicts with a new one(same index
	// but different terms), delete the existing entry and all that
	// follow it. (§5.3)
	i := 0
	for ; i < len(args.Entries) && rf.getLogicalIndex(len(rf.log)-1) >= args.PrevLogIndex+i+1; i++ {
		if rf.getSliceIndex(args.PrevLogIndex+i+1) < 1 {
			continue
		}
		if rf.log[rf.getSliceIndex(args.PrevLogIndex+i+1)].Term != args.Entries[i].Term {
			DPrintf(dFollower, "S%d truncate from %d\n", rf.me, args.PrevLogIndex+i+1)
			rf.log = rf.log[:rf.getSliceIndex(args.PrevLogIndex+i+1)]
			break
		}
	}
	// Append any new entries not already in the log.
	for ; i < len(args.Entries); i++ {
		if rf.getSliceIndex(args.PrevLogIndex+i+1) < 1 {
			continue
		}
		rf.log = append(rf.log, args.Entries[i])
		DPrintf(dAppend, "S%d append %v at %d\n", rf.me, args.Entries[i], rf.getLogicalIndex(len(rf.log)-1))
	}
	rf.persist(nil)

	// If leader Commit > commitIndex, set
	// commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == Leader
	if isLeader {
		index = rf.getLogicalIndex(len(rf.log))
		term = rf.currentTerm
		newEntry := LogEntry{Cmd: command, Term: term}
		rf.log = append(rf.log, newEntry)
		DPrintf(dAppend, "S%d start append %v at %d\n", rf.me, newEntry, index)
		rf.persist(nil)
		rf.triggerHeartBeatTimer()
		//go rf.sync()
	}

	return index, term, isLeader
}

// Replicate entries to follower indefinitely.
func (rf *Raft) replicateEntries(target int, term int, leaderCommit int) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for rf.killed() == false {
		entries := []LogEntry{}

		rf.mu.Lock()
		if rf.state != Leader || term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if rf.getSliceIndex(rf.nextIndex[target]) < 1 {
			// Send an InstallSnapshot RPC if it doesn't have the log entries
			// required to bring a follower up to date.
			rf.mu.Unlock()
			rf.sendSnapshot(target, term)
			continue
		} else {
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		if rf.state != Leader || term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if rf.getSliceIndex(rf.nextIndex[target]) < 1 {
			rf.mu.Unlock()
			continue
		}
		for j := rf.getSliceIndex(rf.nextIndex[target]); j < len(rf.log); j++ {
			entries = append(entries, rf.log[j])
		}
		args = &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[target] - 1,
			PrevLogTerm:  rf.log[rf.getSliceIndex(rf.nextIndex[target]-1)].Term,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		rf.mu.Unlock()

		DPrintf(dLeader, "S%d -> S%d T%d PLI%d PLT%d LC%d %v\n", rf.me, target, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
		// Retries AppendEntries RPCs indefinitely
		for rf.killed() == false && rf.peers[target].Call("Raft.AppendEntries", args, reply) == false {
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
		if reply.Success || rf.killed() {
			break
		}

		rf.mu.Lock()
		if rf.currentTerm > term || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			DPrintf(dTerm, "S%d update T%d to T%d\n", rf.me, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist(nil)
			rf.mu.Unlock()
			return
		}
		// Implement optimization of nextIndex back up.
		if reply.Xterm != -1 {
			hasXterm := false
			for i := args.PrevLogIndex; i >= reply.XIndex; i-- {
				if rf.getSliceIndex(i) < 1 {
					break
				}
				if rf.log[rf.getSliceIndex(i)].Term == reply.Xterm {
					hasXterm = true
					rf.nextIndex[target] = i + 1
					break
				}
			}
			if hasXterm == false {
				rf.nextIndex[target] = reply.XIndex
			}
		} else {
			rf.nextIndex[target] = reply.Xlen + 1
		}
		reply = &AppendEntriesReply{}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if rf.nextIndex[target] < args.PrevLogIndex+len(args.Entries)+1 {
		rf.nextIndex[target] = args.PrevLogIndex + len(args.Entries) + 1
	}
	if rf.matchIndex[target] < args.PrevLogIndex+len(args.Entries) {
		rf.matchIndex[target] = args.PrevLogIndex + len(args.Entries)
	}
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	for N := rf.commitIndex + 1; N < rf.getLogicalIndex(len(rf.log)); N++ {
		if rf.log[rf.getSliceIndex(N)].Term != rf.currentTerm {
			continue
		}
		nCommit := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				nCommit++
			}
		}
		if nCommit > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
		} else {
			break
		}
	}
	rf.mu.Unlock()
}

// Send snapshot to follower indefinitely.
func (rf *Raft) sendSnapshot(target int, term int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getLogicalIndex(0),
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	DPrintf(dSnapshot, "S%d -> S%d Snap T%d LLI%d LLT%d\n", rf.me, target, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	for rf.killed() == false && rf.peers[target].Call("Raft.InstallSnapshot", args, reply) == false {
		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || term != rf.currentTerm || rf.killed() {
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf(dTerm, "S%d update T%d to T%d\n", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist(nil)
		return
	}

	if reply.Success {
		if rf.nextIndex[target] < args.LastIncludedIndex+1 {
			rf.nextIndex[target] = args.LastIncludedIndex + 1
		}
		if rf.matchIndex[target] < args.LastIncludedIndex {
			rf.matchIndex[target] = args.LastIncludedIndex
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// close(rf.applyCh)
	rf.applyCond.Signal()
	//rf.electionTimer.Reset(0)
	//rf.heartbeatTimer.Reset(0)
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
	rf.killCh <- struct{}{}
	rf.killCh <- struct{}{}
	DPrintf(dKill, "S%d killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Reset election timer.
func (rf *Raft) resetElectionTimer() {
	if rf.killed() == false {
		ms := 200 + (rand.Int63() % 200)
		rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond)
		DPrintf(dTimer, "S%d reset %dms\n", rf.me, ms)
	}
}

// Reset heartbeat timer.
func (rf *Raft) triggerHeartBeatTimer() {
	rf.heartbeatTimer.Reset(0)
}

func (rf *Raft) ticker() {
	//ms := 200 + (rand.Int63() % 200)
	//rf.timer = time.NewTimer(time.Duration(ms) * time.Millisecond)

	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
		case <-rf.killCh:
			return
		}

		DPrintf(dTimer, "S%d timeout\n", rf.me)
		rf.startElection()
		rf.resetElectionTimer()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Start an election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader || rf.killed() {
		return
	}

	DPrintf(dVote, "S%d start vote T%d\n", rf.me, rf.currentTerm+1)

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastLogIndex := rf.getLogicalIndex(len(rf.log) - 1)
	lastLogTerm := rf.log[len(rf.log)-1].Term
	grantedVotes := 1

	// rf.resetElectionTimer()

	rf.persist(nil)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		target := i
		go func() {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if rf.peers[target].Call("Raft.RequestVote", args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != term || rf.state != Candidate {
					return
				}
				if reply.VoteGranted {
					grantedVotes++
					DPrintf(dVote, "S%d <- S%d votes %d\n", rf.me, target, grantedVotes)
					if grantedVotes > len(rf.peers)/2 {
						rf.matchIndex = make([]int, len(rf.peers))
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.getLogicalIndex(len(rf.log))
						}
						rf.state = Leader
						DPrintf(dVote, "S%d win vote T%d\n", rf.me, term)
						go rf.heartBeat()
					}
				} else if reply.Term > rf.currentTerm {
					DPrintf(dTerm, "S%d update T%d to T%d\n", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist(nil)
				}
			}
		}()
	}
}

// Reach agreement。
func (rf *Raft) sync() {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	// DPrintf(dLeader, "S%d heartbeat T%d PLI%d PLT%d LC%d []\n", rf.me, rf.currentTerm, prevLogIndex, prevLogTerm, leaderCommit)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		target := i
		go rf.replicateEntries(target, term, leaderCommit)
	}
	rf.mu.Unlock()
}

// Leader do heartbeat periodically.
func (rf *Raft) heartBeat() {
	// Before heartbeat, sleep a while.
	// time.Sleep(time.Duration(50) * time.Millisecond)

	// Check isLeader again.
	//rf.mu.Lock()
	//if rf.state != Leader {
	//	rf.mu.Unlock()
	//	return
	//}
	//// Initialize matchIndex and nextIndex array.
	////rf.matchIndex = make([]int, len(rf.peers))
	////rf.nextIndex = make([]int, len(rf.peers))
	////for i := range rf.nextIndex {
	////	rf.nextIndex[i] = rf.getLogicalIndex(len(rf.log))
	////}
	//rf.mu.Unlock()

	// rf.heartbeatTimer = time.NewTimer(time.Duration(50) * time.Millisecond)
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
		case <-rf.killCh:
			return
		}
		rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.sync()
		// rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
		// time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// Send apply message when committed.
func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()

	for rf.killed() == false {
		for rf.lastApplied == rf.commitIndex && rf.killed() == false {
			rf.applyCond.Wait()
		}
		for rf.lastApplied < rf.commitIndex && rf.killed() == false {
			// Lock for (2D).
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}
			if rf.getSliceIndex(rf.lastApplied) < 0 {
				rf.mu.Unlock()
				break
			}
			rf.lastApplied++
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.getSliceIndex(rf.lastApplied)].Cmd, CommandIndex: rf.lastApplied}
			DPrintf(dApply, "S%d apply %v at %d\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
			rf.mu.Unlock()

			rf.applyCh <- applyMsg
		}
	}
}

// Convert logical index to slice index.
func (rf *Raft) getSliceIndex(logicalIndex int) int {
	return logicalIndex - rf.indexOffset
}

// Convert slice index to logical index.
func (rf *Raft) getLogicalIndex(sliceIndex int) int {
	return sliceIndex + rf.indexOffset
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	rf.killCh = make(chan struct{}, 2)
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	ms := 200 + (rand.Int63() % 200)
	rf.electionTimer = time.NewTimer(time.Duration(ms) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(50) * time.Millisecond)
	go rf.ticker()

	go rf.applier()

	return rf
}
