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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

	lastElectionTime    time.Time
	lastElectionTimeOut time.Duration

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votefor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votefor int
	var log []Entry
	if d.Decode(&term) != nil ||
		d.Decode(&votefor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("error decoding\n")
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.term = term
		rf.votefor = votefor
		rf.log = log
	}
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

	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.term
	reply.VoteGranted = false
	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.downgrade(args.Term)
	}
	reply.Term = rf.term
	if rf.votefor == -1 || rf.votefor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votefor = args.CandidateId
			rf.state = follower
			rf.resetElectionTime()
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
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

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) downgrade(term int) {
	rf.term = term
	rf.state = follower
	rf.votefor = -1
	rf.persist()
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.term
	resp.Success = false
	if req.Term < rf.term {
		return
	}

	rf.resetElectionTime()
	if req.Term > rf.term {
		rf.downgrade(req.Term)
	}

	if req.PrevLogIndex >= len(rf.log) {
		resp.XTerm = -1
		resp.XLen = len(rf.log)
		return
	} else if req.PrevLogTerm != rf.log[req.PrevLogIndex].Term {
		index := req.PrevLogIndex
		for ; index > rf.commitIndex && rf.log[index].Term == rf.log[req.PrevLogIndex].Term; {
			index--
		}
		resp.XTerm = rf.log[req.PrevLogIndex].Term
		resp.XIndex = index + 1
		return
	}
	
	resp.Success = true
	if len(req.Entries) != 0 && len(rf.log) > req.PrevLogIndex + 1 {
		rf.log = rf.log[:req.PrevLogIndex+1]
	}

	rf.log = append(rf.log, req.Entries...)
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(req.LeaderCommit, len(rf.log)-1)
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

func (rf *Raft) StartReplicate() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		go rf.replicate(rf.term)
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 150)
	}
}

func (rf *Raft) replicate(term int) {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			req := AppendEntriesRequest{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server] - 1].Term,
				Entries:      rf.log[rf.nextIndex[server]:], // copy the entries to the next index
				LeaderCommit: rf.commitIndex,
				IsHeartBeat:  false,
			}
			rf.mu.Unlock()
			resp := AppendEntriesResponse{}
			ok := rf.sendAppendEntries(server, &req, &resp)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != leader {
				return
			}
	
			if resp.Success {
				rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.commit()
				return
			} 

			if resp.Term > req.Term {
				rf.downgrade(resp.Term)
				rf.resetElectionTime()
				return
			}

			if resp.Term == rf.term {
				if resp.XTerm == -1 {
					rf.nextIndex[server] = resp.XLen
				} else {
					index := req.PrevLogIndex
					for ; rf.log[index].Term > resp.XTerm && index > 0; {
						index--
					}
					if rf.log[index].Term == resp.XTerm {
						rf.nextIndex[server] = index + 1
					} else {
						rf.nextIndex[server] = resp.XIndex
					}
				}
			}

		}(server)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
	rf.persist()

	index = len(rf.log) - 1
	term = rf.term
	isLeader = rf.state == leader

	return index, term, isLeader
}

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
	for !rf.killed() {
		appliedMsg := make([]ApplyMsg, 0)
		rf.mu.Lock()

		for ; rf.lastApplied < rf.commitIndex; {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].Command,
			}
			appliedMsg = append(appliedMsg, msg)
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsg {
			rf.applyCh <- msg
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state != leader {
			if time.Since(rf.lastElectionTime) > rf.lastElectionTimeOut {
				go rf.leaderelection()
				rf.resetElectionTime()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetElectionTime() {
	rf.lastElectionTime = time.Now()
	rf.lastElectionTimeOut = time.Duration(rand.Intn(150)+300) * time.Millisecond
}

func (rf *Raft) leaderelection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term++
	rf.votefor = rf.me
	rf.persist()

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
								rf.matchIndex[server] = 0
							}
							go rf.StartReplicate()
						}
					}
					rf.mu.Unlock()
				} else if resp.Term > req.Term {
					rf.mu.Lock()
					rf.downgrade(resp.Term)
					rf.mu.Unlock()
				}
			}
		}(server)
	}
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
	rf.votefor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Entry, 1)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.persister = persister
	rf.readPersist(persister.ReadRaftState())
	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
