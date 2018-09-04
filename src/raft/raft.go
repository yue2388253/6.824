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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"


const (
	STATE_LEADER 	= 0
	STATE_CANDIDATE = 1
	STATE_FOLLOWER 	= 2
	ELECT_TIMEOUT	= 300 * time.Millisecond	// Election timeout(ms) (will plus a randomized value)
	HB_INTERVAL		= 150 * time.Millisecond	// Heartbeat interval(ms)
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term 	int
	Index 	int
	Content	string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu       		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state			int

	currentTerm 	int
	votedFor 		int
	log				[]LogEntry

	commitIndex		int
	lastApplied		int

	nextIndex		[]int
	matchIndex 		[]int

	voteNum			int
	heartbeat		chan bool
	becomeLeader	chan bool
	applyMsg		chan ApplyMsg

}

type AppendEntryArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntryReply struct {
	Term			int
	Success 		bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == STATE_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted 	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Follower %v received RequestVote from Candidate %v.\n", rf.me, args.CandidateId)
	switch {
	case args.Term < rf.currentTerm:
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return

	case args.Term > rf.currentTerm:
		rf.state = STATE_FOLLOWER
		DPrintf("Follower %v voted Candidate %v.", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.heartbeat <- true
		return

	case args.Term == rf.currentTerm:
		rf.state = STATE_FOLLOWER
		if (rf.votedFor != -1) {
			reply.VoteGranted = false
			return
		} else {
			DPrintf("Strange, the candidate's(%v) term equal mine(%v). " +
				"This candidate may be outdated.", args.CandidateId, rf.me)
			reply.VoteGranted = false
			return
		}

	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// Do something to handle the reply
	if reply.VoteGranted {
		DPrintf("Get a vote from %v.", server)
		rf.mu.Lock()
		rf.voteNum++
		if rf.voteNum > len(rf.peers) / 2 {
			if rf.state == STATE_CANDIDATE {
				DPrintf("Candidate %v is voted to be the leader.\n", rf.me)
				rf.becomeLeader <- true
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("Candidate %v is broadcasting RequestVote. Current term: %v.",
		rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var args RequestVoteArgs
			var reply RequestVoteReply
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log) - 1].Term
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("sendRequestVote to %v failed.", server)
			}
		}(i)
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("Leader %v is outdated.", args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("Follower %v received AppendEntry. PrevLogIndex > len(rf.log).", rf.me)
		rf.heartbeat <- true
		// TODO
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Follower %v found a conflicting entry.", rf.me)
		reply.Success = false
		// TODO: reply.Term = ?
		rf.log = rf.log[:args.PrevLogIndex]		// Delete the existing entry and all that follow it.
		return
	} else {
		// Find a match entry. Append new entries.
		DPrintf("Follower %v Find a match entry.", rf.me)
		rf.log = rf.log[: args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		rf.heartbeat <- true
		return
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	// TODO: Do something to handle the reply
	if ok {
		if reply.Success == false {
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = STATE_FOLLOWER
				rf.mu.Unlock()
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("Leader %v is broadcasting AppendEntries.", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			var reply AppendEntryReply
			prevLogIndex := rf.matchIndex[server]
			args := AppendEntryArgs{
				Term:			rf.currentTerm,
				LeaderId:		rf.me,
				PrevLogIndex:	prevLogIndex,
				PrevLogTerm:	rf.log[prevLogIndex].Term,
				Entries: 		rf.log[prevLogIndex + 1:],
				LeaderCommit: 	rf.commitIndex,
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntry(server, &args, &reply)
			if !ok {
				DPrintf("sendAppendEntry to %v failed.", server)
			}
			// Do something to handle the reply
		}(i)
	}
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.mu.Unlock()
	rf.broadcastRequestVote()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.heartbeat = make(chan bool, 100)
	rf.becomeLeader = make(chan bool, 1)
	rf.applyMsg = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <- time.After(ELECT_TIMEOUT + time.Duration(rand.Int63() % 300) * time.Millisecond):
					// Hear no heartbeat
					rf.mu.Lock()
					rf.state = STATE_CANDIDATE
					rf.voteNum = 1
					rf.currentTerm++
					rf.mu.Unlock()
					rf.broadcastRequestVote()

				case <- rf.heartbeat:
					// Do nothing
					DPrintf("Follower %v reset the election timeout.", rf.me)

				}

			case STATE_CANDIDATE:
				select {
				case <- rf.becomeLeader:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					rf.broadcastAppendEntries()

				case <- time.After(ELECT_TIMEOUT + time.Duration(rand.Int63() % 300)):
					rf.mu.Lock()
					rf.voteNum = 1
					rf.currentTerm++
					rf.mu.Unlock()
					rf.broadcastRequestVote()
					// Start a new election term

				case <- rf.heartbeat:
					rf.mu.Lock()
					rf.state = STATE_FOLLOWER
					rf.mu.Unlock()

				}

			case STATE_LEADER:
				// How can a leader know it is outdated and thus become a follower
				time.Sleep(HB_INTERVAL)
				rf.broadcastAppendEntries()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
