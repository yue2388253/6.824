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
	"bytes"
	"labgob"
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
	Content	interface{}
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
	becomeFollower	chan bool
	becomeCandidate	chan bool
	applyMsg		chan ApplyMsg
	chanApply		chan bool

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
	LastIndex		int
}

func min(a, b int) (c int) {
	if a > b {
		c = b
	} else {
		c = a
	}
	return
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

func (rf *Raft) GetLastIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) GetLastTerm() int {
	return rf.log[len(rf.log) - 1].Term
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("SaveRaftState\tcurrentTerm:%v\tvotedFor:%v\tlog:%v",
	//	rf.currentTerm, rf.votedFor, rf.log)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
			panic("Decode Error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	DPrintf("Raft %v ReadPersist\tcurrentTerm:%v\tvotedFor:%v\tlog:%v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)
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
	defer rf.persist()

	//DPrintf("Server %v received RequestVote from Candidate %v.\n", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		// This candidate is outdated. Do nothing.
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	upToDate := false

	if args.LastLogTerm > rf.GetLastTerm() ||
		(args.LastLogTerm == rf.GetLastTerm() && args.LastLogIndex >= rf.GetLastIndex()){
			upToDate = true
			//DPrintf("Candidate's log is more up-to-date.")
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if rf.state != STATE_FOLLOWER {
			DPrintf("Hear a higher term from RV%v." +
				" Server %v transform to a follower.",
				args.CandidateId, rf.me)
			rf.state = STATE_FOLLOWER
		}
		DPrintf("Follower %v voted Candidate %v.", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.heartbeat <- true
	} else {
		DPrintf("Server %v rejected Candidate %v.", rf.me, args.CandidateId)
		reply.VoteGranted = false
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
		//DPrintf("Candidate %v get a vote from %v.", rf.me, server)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != STATE_CANDIDATE {
			return ok
		}
		rf.voteNum++
		if rf.voteNum > len(rf.peers) / 2 {
			DPrintf("Candidate %v is voted to be the leader.\n", rf.me)
			rf.becomeLeader <- true
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("Candidate %v is broadcasting RequestVote. Current term: %v.",
		rf.me, rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		var args RequestVoteArgs
		var reply RequestVoteReply
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log) - 1].Term


		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("Candidate %v sendRequestVote to %v failed.", rf.me, server)
			}
		}(i, args, reply)
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("Server %v received AppendEntry from %v.", rf.me, args.LeaderId)

	if args.Term < rf.currentTerm {
		DPrintf("Server %v received AppendEntry. Leader %v is outdated. " +
			"My currentTerm is %v while Leader's is %v.",
			rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//if args.LeaderCommit < rf.commitIndex {
	//	DPrintf("Server %v received AppendEntry. Leader %v is outdated. " +
	//		"My commitIndex is %v while Leader's is %v.",
	//		rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit)
	//	reply.Success = false
	//	reply.Term = rf.currentTerm
	//	return
	//}


	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("Follower %v received AppendEntry. PrevLogIndex > len(rf.log).", rf.me)
		rf.heartbeat <- true
		// TODO: This follower is outdated.
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Follower %v found a conflicting entry.", rf.me)
		reply.Success = false
		// TODO: reply.Term = ?
		rf.log = rf.log[:args.PrevLogIndex]		// Delete the existing entry and all that follow it.
		rf.persist()
		return
	} else {
		// Find a match entry. Append new entries. Avoid two leaders.
		//if rf.state != STATE_FOLLOWER {
		//	DPrintf("Server %v hear a right AppendEntry from Leader %v. " +
		//		"Switch to be a follower.", rf.me, args.LeaderId)
		//	rf.state = STATE_FOLLOWER
		//}
		DPrintf("Follower %v found a match entry at index %v(from Leader %v).",
			rf.me, args.PrevLogIndex, args.LeaderId)
		rf.log = rf.log[: args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		if len(args.Entries) > 0 {
			DPrintf("Now server %v's log is \n\t%v.", rf.me, rf.log)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
			rf.chanApply <- true
		}
		reply.Success = true
		rf.heartbeat <- true
		return
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == STATE_LEADER {
		if reply.Success == false {
			if reply.Term > rf.currentTerm {
				// This Leader might be outdated. Switch to be a follower.
				rf.currentTerm = reply.Term
				rf.persist()
				if rf.state == STATE_LEADER {
					rf.state = STATE_FOLLOWER
					DPrintf("Leader %v received a higher term(%v) from server %v. " +
						"Switch to be a follower", rf.me, reply.Term, server)
				}
			} else {
				if reply.LastIndex > 0 && reply.LastIndex < args.PrevLogIndex {
					rf.nextIndex[server] = reply.LastIndex
				}
			}
		} else {
			//if rf.commitIndex > rf.lastApplied {
			//
			//}

			if len(args.Entries) != 0 {
				// The follower has received the entries and appended them.
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				//DPrintf("matchIndex[%v]: %v\tlen:%v\tnewMatchIndex:%v\trf.commitIndex:%v.",
				//	server, rf.matchIndex[server], len(args.Entries), newMatchIndex, rf.commitIndex)
				//if rf.matchIndex[server] <= rf.commitIndex && newMatchIndex > rf.commitIndex {

				if rf.matchIndex[server] > rf.commitIndex {
					// Leader may need to update commitIndex
					//DPrintf("2")
					count := 1			// From itself
					newCommitIndex := rf.matchIndex[server]
					for i, matchIndex := range rf.matchIndex {
						if i == rf.me {
							continue
						}
						if matchIndex > rf.commitIndex {
							//DPrintf("After send AppendEntry to %v, " +
							//	"leader %v found a higher matchIndex.", server, rf.me)
							count++
							newCommitIndex = min(newCommitIndex, matchIndex)
							//DPrintf("count: %v\tcurrentTerm: %v\tnewCommitIndex:%v",
							//	count, rf.currentTerm, newCommitIndex)
							if count > len(rf.peers) / 2 && rf.log[newCommitIndex].Term == rf.currentTerm {
								//DPrintf("2")
								//DPrintf("matchIndex[%v]: %v\tlen:%v\tnewMatchIndex:%v\trf.commitIndex:%v.",
								//	server, rf.matchIndex[server], len(args.Entries), newMatchIndex, rf.commitIndex)
								// rf.chanApply <- true
								rf.commitIndex = newCommitIndex
								DPrintf("Leader %v update commitIndex. Now is %v.",
									rf.me, rf.commitIndex)
							}
						}
					}
				}
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Leader %v is broadcasting AppendEntries. CurrentTerm: %v",
		rf.me, rf.currentTerm)
	//newLastApplied := rf.lastApplied
	if rf.commitIndex > rf.lastApplied {
		DPrintf("There might be some logs for Leader %v to be applied.", rf.me)
		num := 1
		for index, matchIndex := range rf.matchIndex {
			if index == rf.me {
				continue
			}
			if matchIndex >= rf.commitIndex {
				num++
				//newLastApplied = min(matchIndex, newLastApplied)
				if num > len(rf.peers) / 2 {
					DPrintf("Leader %v applied msg.", rf.me)
					rf.chanApply <- true
					break
				}
			}
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		if rf.state == STATE_LEADER && i != rf.me {
			var reply AppendEntryReply
			prevLogIndex := rf.matchIndex[i]
			args := AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				LeaderCommit: rf.commitIndex,
			}
			if prevLogIndex >= len(rf.log) {
				args.Entries = nil
			} else {
				args.Entries = rf.log[prevLogIndex+1:]
			}
			go func(server int, args AppendEntryArgs, reply AppendEntryReply) {
				ok := rf.sendAppendEntry(server, &args, &reply)
				if !ok {
					DPrintf("Leader %v sendAppendEntry to %v failed.", rf.me, server)
				}
			}(i, args, reply)
		}
	}
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.state = STATE_CANDIDATE
	rf.voteNum = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.state == STATE_LEADER)
	if isLeader == false {
		return index, term, isLeader
	}

	// Your code here (2B).
	index = len(rf.log)
	term = rf.currentTerm
	logEntry := LogEntry{
		Term:		term,
		Index:		index,
		Content:	command,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.log) - 1
	DPrintf("Leader %v received log from client.\n\t\t" +
		"Now the log is %v.", rf.me, rf.log)

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
	rf.becomeFollower = make(chan bool, 1)
	rf.becomeCandidate= make(chan bool, 1)
	rf.chanApply = make(chan bool, 1)
	rf.applyMsg = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			switch rf.state {
			// TODO: remove some unnecessary codes such as rf.becomeFollower
			case STATE_FOLLOWER:
				select {
				case <- time.After(ELECT_TIMEOUT + time.Duration(rand.Int63() % 300) * time.Millisecond):
					// Hear no heartbeat
					rf.startNewElection()

				case <- rf.heartbeat:
					// Do nothing

				case <- rf.becomeCandidate:
					// Received a AppendEntry whose commitIndex is lower than me.
					rf.startNewElection()

				}

			case STATE_CANDIDATE:
				select {
				case <- rf.becomeLeader:
					rf.mu.Lock()
					rf.voteNum = 0
					rf.state = STATE_LEADER
					rf.votedFor = -1
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					rf.broadcastAppendEntries()

				case <- time.After(ELECT_TIMEOUT + time.Duration(rand.Int63() % 300)):
					rf.startNewElection()

				case <- rf.heartbeat:
					// There is already a leader.
					rf.mu.Lock()
					rf.state = STATE_FOLLOWER
					rf.mu.Unlock()

				case <- rf.becomeFollower:
					// Hear a more higher term.
					rf.mu.Lock()
					rf.state = STATE_FOLLOWER
					rf.mu.Unlock()

				}

			case STATE_LEADER:
				select {
				case <- rf.becomeFollower:
					// Hear a higher term.
					rf.mu.Lock()
					rf.state = STATE_FOLLOWER
					rf.mu.Unlock()

				case <- time.After(HB_INTERVAL):
					rf.broadcastAppendEntries()

				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <- rf.chanApply:
				// Apply those logs between lastApplied and commitIndex, then set lastApplied.
				rf.mu.Lock()
				for _, commitEntry := range rf.log[rf.lastApplied + 1: rf.commitIndex + 1] {
					DPrintf("Server %v send msg %v to applyMsg.", rf.me, commitEntry)
					rf.applyMsg <- ApplyMsg{true, commitEntry.Content, commitEntry.Index}
				}
				rf.lastApplied = rf.commitIndex
				DPrintf("Server %v update lastApplied, now is %v.", rf.me, rf.lastApplied)
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
