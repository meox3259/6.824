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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	candidate = 0
	follower  = 1
	leader    = 2
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

type Entry struct {
	Term    int
	Command interface{}
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
	/*-------2A----------*/
	term  int
	state int

	lastHeartBeaten        time.Time
	lastHeartBeatenTimeOut time.Duration

	lastElectionTime    time.Time
	lastElectionTimeOut time.Duration

	lastSendHeartBeaten        time.Time
	lastSendHeartBeatenTimeOut time.Duration

	lastAppendEntries        time.Time
	lastAppendEntriesTimeOut time.Duration

	lastAppliedTime    time.Time
	lastAppliedTimeOut time.Duration

	votefor int

	/*-------2B----------*/
	log []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.term, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	if args.Term < rf.term || (args.Term == rf.term && rf.votefor != -1 && rf.votefor != args.CandidateId) {
		return
	}
	if rf.log[len(rf.log)-1].Term > args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {
		return
	}
	if args.Term > rf.term {
		rf.votefor = -1
		rf.term = args.Term
		rf.state = follower
	}
	reply.VoteGranted = true
	rf.votefor = args.CandidateId
	rf.resetElectionTime()
	rf.state = follower
}

type AppendEntriesRequest struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry

	IsHeartBeat bool

	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool

	NextIndex int
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.term
	resp.Success = false
	if req.Term < rf.term {
		return
	}
	if req.IsHeartBeat {
		resp.Success = true
		rf.resetHeartBeaten()
		rf.state = follower
		rf.term = req.Term
		return
	}
	if req.PrevLogIndex < rf.commitIndex {
		return
	}
	if len(rf.log) <= req.PrevLogIndex {
		resp.NextIndex = len(rf.log)
		return
	}
	if rf.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		index := req.PrevLogIndex
		for ; index > rf.commitIndex && rf.log[index].Term == rf.log[req.PrevLogIndex].Term; {
			index--
		}
		resp.NextIndex = index + 1
		return
	}
	resp.Success = true
	rf.log = append(rf.log[:req.PrevLogIndex+1], req.Entries...)
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = req.LeaderCommit
	}
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
	return ok
}

func (rf *Raft) commit() {
	for i := len(rf.log) - 1; i > rf.commitIndex && rf.log[i].Term == rf.term; i-- {
		count := 1
		for server := 0; server < len(rf.peers); server++ {
			if rf.me == server {
				continue
			}
			if rf.matchIndex[server] >= i {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.commitIndex = i
			break
		}
	}
}

func (rf *Raft) replicate() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			req := AppendEntriesRequest{
				Term:         rf.term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server] - 1].Term,
				Entries:      rf.log[rf.nextIndex[server]:], // copy the entries to the next index
				LeaderCommit: rf.commitIndex,
				IsHeartBeat:  false,
			}
			resp := AppendEntriesResponse{}
			ok := rf.sendAppendEntries(server, &req, &resp)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !resp.Success {
				if resp.Term > req.Term {
					rf.term = resp.Term
					rf.state = follower
					rf.votefor = -1
				} else {
					rf.nextIndex[server] = resp.NextIndex
				}
			} else {
				rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = len(rf.log) - 1
				if len(req.Entries) > 0 && req.Entries[len(req.Entries)-1].Term == rf.term {
					rf.commit()
				}
			}
		}(server)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	if rf.state != leader {
		return -1, -1, rf.state == leader
	}
	entry := Entry{
		Term:    rf.term,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)
	go rf.replicate()

	index = len(rf.log) - 1
	term = rf.term
	isLeader = rf.state == leader

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) applier() {
	for rf.killed() == false {
		if time.Since(rf.lastAppliedTime) > rf.lastAppliedTimeOut {
			rf.mu.Lock()
			for i := rf.lastApplied; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.log[i].Command,
				}
				rf.applyCh <- msg
				rf.lastApplied = i
			}
			rf.resetAppliedTime()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == follower {
			if time.Since(rf.lastHeartBeaten) > rf.lastHeartBeatenTimeOut {
				rf.state = candidate
			}
		}
		if rf.state == candidate {
			if time.Since(rf.lastElectionTime) > rf.lastElectionTimeOut {
				go rf.leaderelection()
				rf.resetElectionTime()
			}
		}
		if rf.state == leader {
			if time.Since(rf.lastSendHeartBeaten) > rf.lastSendHeartBeatenTimeOut {
				go rf.sendheartbeats(rf.term)
			}

			if time.Since(rf.lastAppendEntries) > rf.lastAppendEntriesTimeOut {
				go rf.replicate()
				rf.resetAppendEntriesTime()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetHeartBeaten() {
	rf.lastHeartBeaten = time.Now()
	rf.lastHeartBeatenTimeOut = time.Duration(rand.Intn(200)+200) * time.Millisecond
}

func (rf *Raft) resetSendHeartBeaten() {
	rf.lastSendHeartBeaten = time.Now()
	rf.lastSendHeartBeatenTimeOut = time.Duration(rand.Intn(100)+100) * time.Millisecond
}

func (rf *Raft) resetElectionTime() {
	rf.lastElectionTime = time.Now()
	rf.lastElectionTimeOut = time.Duration(rand.Intn(300)+300) * time.Millisecond
}

func (rf *Raft) resetAppendEntriesTime() {
	rf.lastAppendEntries = time.Now()
	rf.lastAppendEntriesTimeOut = time.Duration(rand.Intn(100)+100) * time.Millisecond // to modify
}

func (rf *Raft) resetAppliedTime() {
	rf.lastAppliedTime = time.Now()
	rf.lastAppliedTimeOut = time.Duration(rand.Intn(100)+100) * time.Millisecond
}

func (rf *Raft) leaderelection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term++
	rf.votefor = rf.me
	rf.state = candidate
	vote := 1

	req := RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			resp := RequestVoteReply{}
			if rf.sendRequestVote(server, &req, &resp) {
				if resp.VoteGranted {
					rf.mu.Lock()
					vote++
					if vote >= len(rf.peers)/2+1 {
						if rf.term == req.Term && rf.state == candidate {
							rf.state = leader
							for server := 0; server < len(rf.peers); server++ {
								rf.nextIndex[server] = len(rf.log)
								rf.matchIndex[server] = len(rf.log) - 1
							}
							go rf.sendheartbeats(rf.term)
							go rf.replicate()
						}
					}
					rf.mu.Unlock()
				} else if resp.Term > req.Term {
					rf.mu.Lock()
					rf.term = resp.Term
					rf.state = follower
					rf.votefor = -1
					rf.mu.Unlock()
				}
			}
		}(server)
	}
}

func (rf *Raft) sendheartbeats(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.term {
		return
	}

	if rf.state != leader {
		return
	}

	req := AppendEntriesRequest{
		Term:         rf.term,
		LeaderId:     rf.me,
		Entries:      []Entry{}, // copy the entries to the next index
		LeaderCommit: rf.commitIndex,
		IsHeartBeat:  true,
	}

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			resp := AppendEntriesResponse{}
			ok := rf.sendAppendEntries(server, &req, &resp)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if resp.Term > rf.term {
				rf.term = resp.Term
				rf.state = follower
				rf.votefor = -1
			}
		}(server)
	}
	rf.resetSendHeartBeaten()
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
	rf.term = 0
	rf.state = follower

	rf.resetElectionTime()
	rf.resetHeartBeaten()
	rf.resetSendHeartBeaten()
	rf.resetAppendEntriesTime()
	rf.resetAppliedTime()
	rf.votefor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Entry, 1)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
