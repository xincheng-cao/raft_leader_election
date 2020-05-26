package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)
// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command  interface{}
	CommandIndex int
}

type Log struct {
	Index  int
	Term  int
	Cmd interface{}
}

type ChanVar struct {
	ChanLeader chan bool
	Heartbeat chan bool
	Timeout chan bool
}

type AppendEntries struct {
	Term int
	Index int
	Logs []Log
}

type ReAppendEntries struct {
	Term int
	Succ bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg         // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	curTerm int
	votedFor int
	logs []Log
	status string
	numVotes int
	chanVar ChanVar
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term=rf.curTerm
	if(rf.status=="leader") {
		isleader=true
	}else {
		isleader=false
	}
	return term,isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (4).
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
	// Your code here (4).
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
	// Your data here (3, 4).
	Term int
	Index int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3).
	Term int
	Succ bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(fw *RequestVoteArgs, re *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	re.Succ = false
	if(fw.Term>rf.curTerm){
		rf.curTerm=fw.Term
		rf.status="follower"
		rf.votedFor=fw.Index
		rf.chanVar.Timeout <- true
		re.Succ=true
	}
	re.Term=rf.curTerm
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
	ok := rf.peers[server].Call("Raft.RequestVote",args,reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if(rf.status=="leader"||rf.status=="follower"){
		return ok
	}else{
		if(ok==true&&reply.Succ==true){
			rf.numVotes+=1
			numPeers:= len(rf.peers)
			if (rf.numVotes>numPeers/2){
				rf.status="leader"
				rf.chanVar.ChanLeader <- true
			}
		}
	}
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

	// Your code here (4).

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

	// Your initialization code here (3, 4).
	rf.curTerm=0
	rf.votedFor=-1
	rf.logs=*new([]Log)
	tempLog:=Log{0,0,nil}
	rf.logs=append(rf.logs,tempLog)
	rf.numVotes=0
	rf.status="follower"
	tempChanVar:=ChanVar{ make(chan bool, 200), make(chan bool, 200), make(chan bool, 200)}
	rf.chanVar=tempChanVar

	go rf.StatusHandler()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft)StatusHandler() {
	for(true){
		timeout:=(1*time.Millisecond)*time.Duration(int(300*(1+rand.Float32())))
		rf.mu.Lock()
		status:=rf.status
		rf.mu.Unlock()
		switch status {
		case "follower":
			select {
			case <-rf.chanVar.Heartbeat:
			case <-rf.chanVar.Timeout:
			case <-time.After(timeout):
				rf.SetStatus("candidate")
			}
		case "candidate":
			rf.VoteForMe()
			go rf.DoElection()
			select {
			case <-time.After(timeout):
			case <-rf.chanVar.Heartbeat:
				rf.SetStatus("follower")
			case <-rf.chanVar.ChanLeader:
				rf.SetStatus("leader")
			}
		case "leader":
			rf.FwHeartbeat2Leader()
			time.Sleep(100*time.Millisecond)
		}
	}
}

func (rf *Raft) SetStatus(string2 string){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status=string2
}

func (rf *Raft) VoteForMe(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.curTerm++
	rf.votedFor=rf.me
	rf.numVotes=1
}

func (rf *Raft) DoElection() {
	rf.mu.Lock()
	fw:=RequestVoteArgs{rf.curTerm,rf.me}
	rf.mu.Unlock()

	for peer:=0;peer< len(rf.peers);peer++ {
		if (peer!=rf.me && rf.status=="candidate"){
			var re RequestVoteReply
			go rf.sendRequestVote(peer, &fw, &re)
		}
	}
}

func (rf *Raft) FwHeartbeat2Leader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server:=0;server< len(rf.peers);server++{
		if (server!=rf.me && rf.status=="leader"){
			fw:=AppendEntries{rf.curTerm,rf.me,nil}
			var re ReAppendEntries
			go rf.FwHeartbeat(server, &fw, &re)
		}
	}
}

func (rf *Raft)FwHeartbeat(server int, fw * AppendEntries, re * ReAppendEntries){
	ok:=rf.peers[server].Call("Raft.AppendEntriesHandler", fw, re)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (ok==true && re.Term> rf.curTerm) {
		rf.curTerm=re.Term
		rf.status= "follower"
		rf.votedFor= -1
	}
}

func (rf *Raft)AppendEntriesHandler(fw * AppendEntries, re * ReAppendEntries){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (fw.Term>rf.curTerm){
		rf.curTerm=fw.Term
		rf.status="follower"
		rf.votedFor=-1
		rf.chanVar.Heartbeat <- true
		re.Succ=true
		re.Term=fw.Term
	}else if (fw.Term==rf.curTerm) {
		rf.chanVar.Heartbeat <- true
		re.Term = fw.Term
		re.Succ=true
	}else{
		re.Term = rf.curTerm
		re.Succ=false
	}
}
