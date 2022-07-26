package lab2

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
	// "math/rand"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type State int8

const(
	Follower State = iota // auto increment itself and variables defined below it. Follower = 0
	Candidate // Candidate = 1
	Leader // Leader = 2
)

//
// A Go object implementing a single Raft peer.
// A server in a raft cluster is either a leader or a follower, 
// and can be a candidate in the precise case of an election (leader unavailable).
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state		State // Leader, follower or candidate
	currentTerm	int // current term # of this raft server
	votedFor    int // candidateId that received vote in current term
	log			[]LogEntry // log entries. Each entry contains command for state machine,
						   // and term when entry was received by leader (index starts at 1)
	// heartbeatTimer	*time.Timer
	// electionTimer	*time.Timer
	
	voteCounts	int
	
	electionTimeout	int
	latestHeartTime	time.Time
	heartbeatPeriod		int
	
	// channels
	electionTimeoutChannel	chan	bool // true means running out of time and should start election
	heartbeatPeriodChannel	chan	bool // true means should start heartbeat
	
	commitIndex	int // index of highest log entry known to be commited (initialized to be 0)
	lastApplied int // index of highest log entry applied to state machine (initialized to be 0)
					// ^ these two indexes both increase monotonically
		
	// only leaders
	nextIndex	[]int // for each server, index of the next log entry to send to that server
					  // initizlied to leader last log index + 1
	matchIndex	[]int // for each server, index of the highest log entry known to be replicated on server
}

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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

type LogEntry struct {
	Term	int
	Index	int
	Command	interface{}
}

// AppendEntries RPC model
type AppendEntryArgs struct {
	Term			int
	LeaderId 		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntryReply struct {
	Term			int
	Success			bool
}

// AppendEntries RPC Handler
// if len(args.entries) == 0 -> heartbeats
func (rf *Raft) AppendEntriesRPCHandler(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	
	if rf.currentTerm <= args.Term {
		if len(args.Entries) == 0 {
			reply.Success = true
			rf.resetElectionTimer()
			rf.votedFor = -1
			rf.convertTo(Follower)
		}
	} else {
		reply.Success = false
	}
	
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPCHandler", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		 int // candidate's term
	CandidateId  int // candidate requesting vote
 	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int  // current term for candidate to update itself
	VoteGranted	bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	rf.currentTerm = args.Term
	rf.convertTo(Follower)
	rf.votedFor = -1
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
	return ok
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) startElection() {
	// start election you dumb
	rf.mu.Lock()
	rf.convertTo(Candidate)
	rf.currentTerm++
	rf.votedFor = rf.me
	nVotes := 1
	rf.resetElectionTimer()
	
	rf.mu.Unlock()
	
	go func (nVotes *int)  {
		var wg sync.WaitGroup
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			
			rf.mu.Lock()
			wg.Add(1)
			
			args := RequestVoteArgs {
				Term: rf.currentTerm,
				CandidateId: rf.me,
			}
			
			rf.mu.Unlock()
			var reply RequestVoteReply
			go func (i int, args *RequestVoteArgs, reply *RequestVoteReply)  {
				defer wg.Done()
				ok := rf.sendRequestVote(i, args, reply)
				
				if !ok {
					return
				}
				
				if !reply.VoteGranted {
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.convertTo(Follower)
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					*nVotes++
					if rf.state == Candidate && *nVotes >= len(rf.peers) / 2 + 1 {
						rf.currentTerm = args.Term
						rf.convertTo(Leader)
						go rf.broadcastHeartbeat()
					}
					
					rf.mu.Unlock()
				}
			} (i, &args, &reply)
		}
	} (&nVotes)
}


func (rf *Raft) broadcastHeartbeat() {
	if rf.state != Leader {
		return
	}
	
	var wg sync.WaitGroup
	rf.mu.Lock()
	rf.latestHeartTime = time.Now()
	rf.mu.Unlock()
	
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		wg.Add(1)
		go func (i int)  {
			rf.mu.Lock()
			entries := make([]LogEntry, 0)
			args := AppendEntryArgs {
				Term: rf.currentTerm,
				Entries: entries,
			}
			
			rf.mu.Unlock()
			
			var reply AppendEntryReply
			if rf.sendAppendEntries(i, &args, &reply) {
				if !reply.Success {
					rf.mu.Lock()
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.convertTo(Follower)
					rf.mu.Unlock()
				}
			}
		} (i)
	}
	
	wg.Wait()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <- rf.electionTimeoutChannel:
			rf.mu.Lock()
			go rf.startElection()
			rf.mu.Unlock()
		case <- rf.heartbeatPeriodChannel:
			rf.mu.Lock()
			go rf.broadcastHeartbeat()
			rf.mu.Unlock()
		}
	}
}


func (rf *Raft) electionTimeoutTick() {
	for {
		if rf.state != Leader {
			rf.mu.Lock()
			if time.Since(rf.latestHeartTime) >= time.Duration(rf.electionTimeout) * time.Millisecond {
				rf.electionTimeoutChannel <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 20)
		}
	}
}

func (rf *Raft) heartbeatPeriodTick() {
	for {
		if rf.state == Leader {
			rf.mu.Lock()
			rf.heartbeatPeriodChannel <- true
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(rf.heartbeatPeriod))
		}
	}
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.voteCounts = 0
	rf.heartbeatPeriod = 100 // each 100ms sends a heartbeat
	
	rf.resetElectionTimer()
	rf.electionTimeoutChannel = make(chan bool)
	rf.heartbeatPeriodChannel = make(chan bool)
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionTimeoutTick()
	go rf.heartbeatPeriodTick()
	
	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}


func (rf *Raft) convertTo(state State) {
	if rf.state == state {
		return
	}
	
	rf.state = state
}

func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = 150 + rand.Intn(150)
	rf.latestHeartTime = time.Now()
}