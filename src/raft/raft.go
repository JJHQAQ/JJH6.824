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
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	// "log"
	"math/rand"
	"time"
)

var logcount int

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

type CmdEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	termmu      sync.Mutex
	term        int
	statusmu    sync.Mutex
	status      int //0: follower    1:  condidate    2: leader
	votemapmu   sync.Mutex
	votemaxterm int

	// votemaxindex chan int

	heartbeat      chan int
	getvoterequest chan int

	cmdmu    sync.Mutex
	cmdarray []CmdEntry

	commiteCh chan int

	commitemu     sync.Mutex
	commitedindex int

	maxcommitedIndex int

	taskCh chan int

	realtaskCh    chan int
	realtaskcount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.statusmu.Lock()
	isleader = rf.status == 2
	rf.statusmu.Unlock()
	rf.termmu.Lock()
	term = rf.term
	rf.termmu.Unlock()
	return term, isleader
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
	Term             int
	CandidateId      int
	MaxCommitedIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term             int
	MaxCommitedIndex int
	VoteGranted      bool
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.termmu.Lock()
	rf.commitemu.Lock()
	reply.Term = rf.term
	reply.MaxCommitedIndex = rf.commitedindex
	rf.commitemu.Unlock()
	if args.MaxCommitedIndex >= reply.MaxCommitedIndex {
		rf.votemapmu.Lock()
		if rf.votemaxterm < args.Term {
			// log.Printf("node %v (%v %v) vote for node %v (%v %v)",rf.me,rf.term,rf.commitedindex,args.CandidateId,args.Term,args.MaxCommitedIndex)
			reply.VoteGranted = true
			rf.votemaxterm = args.Term
		} else {
			reply.VoteGranted = false

		}
		rf.votemapmu.Unlock()
	} else {
		reply.VoteGranted = false
	}
	rf.termmu.Unlock()
	// log.Printf("node %d(%d %d) vote for %d %v",rf.me,reply.Term,reply.MaxCommitedIndex,args.CandidateId,args.Term)
	if reply.VoteGranted {
		// go func(){
		rf.getvoterequest <- args.Term
		// }()
	}
}

type AppendEntriesArgs struct {
	Term          int
	Empty         bool
	Commitedindex int
	LeaderID      int
	CurrentCmd    CmdEntry
	LastCmdIndex  int
	LastCmdTerm   int
}

type AppendEntriesReply struct {
	Term          int
	Accept        bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Accept = false
	// log.Printf("term:%d node(%d) %d get entries",rf.term,rf.status,rf.me)
	rf.termmu.Lock()
	reply.Term = rf.term
	rf.termmu.Unlock()

	if args.Term >= reply.Term {
		go func() {
			rf.heartbeat <- args.Term
		}()
	} else {
		reply.Accept = false
		return
	}
	rf.commitemu.Lock()
	if args.CurrentCmd.Index != -1 && args.CurrentCmd.Index < rf.commitedindex {
		rf.commitemu.Unlock()
		reply.Accept = true
		reply.ConflictIndex = rf.commitedindex
		return
	}
	rf.commitemu.Unlock()
	if args.Empty {
		reply.Accept = true

		if args.CurrentCmd.Index == -1 {
			//do nothing
		} else {
			rf.cmdmu.Lock()
			if args.CurrentCmd.Index < len(rf.cmdarray) {
				if args.CurrentCmd.Term == rf.cmdarray[args.CurrentCmd.Index].Term {
					rf.commitemu.Lock()
					lastcommited := rf.commitedindex
					rf.commitedindex = max(rf.commitedindex, min(args.Commitedindex, len(rf.cmdarray)-1))
					newcommited := rf.commitedindex
					rf.commitemu.Unlock()
					for i := lastcommited + 1; i <= newcommited; i++ {
						applymsg := ApplyMsg{CommandValid: true, Command: rf.cmdarray[i].Command, CommandIndex: i + 1}
						// log.Printf("%d : %v",rf.me,applymsg)
						// log.Printf("node %d apply : %v",rf.me,applymsg)
						rf.applyCh <- applymsg
					}
				} else {
					// for i:=len(rf.cmdarray)-1;i>=0;i-- {
					// 	if (rf.cmdarray[i].Index==args.LastCmdIndex&&rf.cmdarray[i].Term==args.LastCmdTerm) {
					// 		sameindex = i
					// 		break
					// 	}
					// }
					reply.ConflictTerm = rf.cmdarray[args.CurrentCmd.Index].Term

					for i := args.CurrentCmd.Index; i >= 0; i-- {
						if rf.cmdarray[i].Term == reply.ConflictTerm {
							reply.ConflictIndex = i
						} else {
							break
						}
					}

					reply.Accept = false
				}

			} else {

				reply.ConflictIndex = len(rf.cmdarray)
				reply.Accept = false
			}
			rf.cmdmu.Unlock()
		}
		return
	}

	rf.cmdmu.Lock()
	cmdlen := len(rf.cmdarray)
	if args.CurrentCmd.Index > cmdlen {

		reply.ConflictIndex = len(rf.cmdarray)
		reply.Accept = false
		rf.cmdmu.Unlock()
		return
	}
	if args.CurrentCmd.Index == cmdlen {
		if cmdlen == 0 || (cmdlen > 0 && args.LastCmdIndex == rf.cmdarray[args.CurrentCmd.Index-1].Index && args.LastCmdTerm == rf.cmdarray[args.CurrentCmd.Index-1].Term) {
			reply.Accept = true
			rf.cmdarray = append(rf.cmdarray, args.CurrentCmd)
			rf.commitemu.Lock()
			lastcommited := rf.commitedindex
			rf.commitedindex = max(rf.commitedindex, min(args.Commitedindex, len(rf.cmdarray)-1))
			newcommited := rf.commitedindex
			rf.commitemu.Unlock()
			// if (rf.commitedindex!=lastcommited){
			// 	log.Printf("add: node %v get AppendEntries from %v  %v -> %v",rf.me,args.LeaderID,lastcommited,rf.commitedindex)
			// 	}
			for i := lastcommited + 1; i <= newcommited; i++ {
				applymsg := ApplyMsg{CommandValid: true, Command: rf.cmdarray[i].Command, CommandIndex: i + 1}
				// log.Printf("node %d apply : %v",rf.me,applymsg)
				rf.applyCh <- applymsg
			}

		} else {

			reply.ConflictTerm = rf.cmdarray[args.CurrentCmd.Index-1].Term

			for i := args.CurrentCmd.Index - 1; i >= 0; i-- {
				if rf.cmdarray[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
				} else {
					break
				}
			}

			reply.Accept = false
		}
		rf.cmdmu.Unlock()
		return
	}

	if args.CurrentCmd.Index < cmdlen {
		if args.CurrentCmd.Index == 0 {

			samepoint := 0
			if rf.cmdarray[args.CurrentCmd.Index].Index == args.CurrentCmd.Index && rf.cmdarray[args.CurrentCmd.Index].Term == args.CurrentCmd.Term {
				samepoint = args.CurrentCmd.Index
			} else {
				rf.cmdarray = make([]CmdEntry, 0)
				rf.cmdarray = append(rf.cmdarray, args.CurrentCmd)
				samepoint = len(rf.cmdarray) - 1
			}

			reply.Accept = true
			rf.commitemu.Lock()
			lastcommited := rf.commitedindex
			rf.commitedindex = max(rf.commitedindex, min(args.Commitedindex, samepoint))
			newcommited := rf.commitedindex
			rf.commitemu.Unlock()

			for i := lastcommited + 1; i <= newcommited; i++ {
				applymsg := ApplyMsg{CommandValid: true, Command: rf.cmdarray[i].Command, CommandIndex: i + 1}
				// log.Printf("node %d apply : %v",rf.me,applymsg)
				rf.applyCh <- applymsg
			}
		} else {
			if args.LastCmdIndex == rf.cmdarray[args.CurrentCmd.Index-1].Index && args.LastCmdTerm == rf.cmdarray[args.CurrentCmd.Index-1].Term {
				samepoint := 0
				if rf.cmdarray[args.CurrentCmd.Index].Index == args.CurrentCmd.Index && rf.cmdarray[args.CurrentCmd.Index].Term == args.CurrentCmd.Term {
					samepoint = args.CurrentCmd.Index
				} else {
					rf.cmdarray = rf.cmdarray[:args.CurrentCmd.Index]
					rf.cmdarray = append(rf.cmdarray, args.CurrentCmd)
					samepoint = len(rf.cmdarray) - 1
				}

				reply.Accept = true
				rf.commitemu.Lock()
				lastcommited := rf.commitedindex
				rf.commitedindex = max(rf.commitedindex, min(args.Commitedindex, samepoint))
				newcommited := rf.commitedindex
				rf.commitemu.Unlock()

				for i := lastcommited + 1; i <= newcommited; i++ {
					applymsg := ApplyMsg{CommandValid: true, Command: rf.cmdarray[i].Command, CommandIndex: i + 1}
					// log.Printf("node %d apply : %v",rf.me,applymsg)
					rf.applyCh <- applymsg
				}
				// log.Printf("node %v get AppendEntries from %v   : %v",rf.me,args.LeaderID,rf.commitedindex)

			} else {
				reply.ConflictTerm = rf.cmdarray[args.CurrentCmd.Index-1].Term

				for i := args.CurrentCmd.Index - 1; i >= 0; i-- {
					if rf.cmdarray[i].Term == reply.ConflictTerm {
						reply.ConflictIndex = i
					} else {
						break
					}
				}
				reply.Accept = false
			}

		}
		rf.cmdmu.Unlock()
		return
	}
	return
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
	// log.Printf("node %d RV to node %d",rf.me,server)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// log.Printf("node %d AE to node %d",rf.me,server)
	// log.Printf("send one to %v",server)
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
	rf.statusmu.Lock()
	if rf.status != 2 {
		isLeader = false
	}
	rf.statusmu.Unlock()
	if !isLeader {
		return index, term, isLeader
	}

	rf.cmdmu.Lock()
	rf.termmu.Lock()
	term = rf.term
	rf.termmu.Unlock()
	index = len(rf.cmdarray)
	cmd := CmdEntry{Command: command, Term: term, Index: index}
	rf.cmdarray = append(rf.cmdarray, cmd)
	rf.cmdmu.Unlock()
	// go func(){
	rf.taskCh <- cmd.Index
	// }()
	return index + 1, term, isLeader
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
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.statusmu.Lock()

		if rf.status == 0 {
			rf.statusmu.Unlock()
			timeout := time.Duration(rand.Intn(650))*time.Millisecond + 350*time.Millisecond
			select {
			case <-time.After(timeout):
				// log.Printf("term:%d node(follower) %d become candidate",rf.term,rf.me)
				rf.commitemu.Lock()
				commited := rf.commitedindex
				rf.commitemu.Unlock()
				if rf.maxcommitedIndex > commited {
					continue
				}
				rf.statusmu.Lock()
				rf.status = 1
				rf.statusmu.Unlock()
			case term := <-rf.heartbeat:
				rf.termmu.Lock()
				if term > rf.term {
					rf.term = term
				}
				rf.termmu.Unlock()
			case term := <-rf.getvoterequest:
				rf.termmu.Lock()
				if term > rf.term {
					// log.Printf("getvoterequest: term:%d>%d node(candidate) %d become follower",term,rf.term,rf.me)
					rf.term = term
				}
				rf.termmu.Unlock()
			case <-rf.taskCh:
				continue
			}
		} else if rf.status == 1 {
			rf.statusmu.Unlock()
			candidateover := make(chan int)
			votecount := make(chan int)
				rf.termmu.Lock()
				rf.votemapmu.Lock()
				rf.term = rf.votemaxterm + 1
				currentterm := rf.term
				rf.votemaxterm = currentterm
				rf.votemapmu.Unlock()
				rf.termmu.Unlock()
				rf.commitemu.Lock()
				// log.Printf("node %d (candidate) increate term:%d->%d with commiteindex: %d",rf.me,currentterm-1,currentterm,rf.commitedindex)
				rf.commitemu.Unlock()
				rf.commitemu.Lock()
				args := RequestVoteArgs{Term: currentterm, CandidateId: rf.me, MaxCommitedIndex: rf.commitedindex}
				rf.commitemu.Unlock()
				reply := RequestVoteReply{}
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}

					go func(index int, args RequestVoteArgs, reply RequestVoteReply) {
						// log.Printf("node %d send VR for node %d",rf.me,index)
						ok := rf.sendRequestVote(index, &args, &reply)
						if ok {
							if reply.VoteGranted {
								// log.Printf("node %d get true vote from %d",rf.me,index)
								votecount <- 2
							} else {
								// log.Printf("node %d get false vote for commiteindex: %d",rf.me,reply.MaxCommitedIndex)
								// log.Printf("node %d get false vote from %d  (%v  %v)",rf.me,index,reply.Term,reply.MaxCommitedIndex)
								votecount <- (-reply.MaxCommitedIndex)
							}
						} else {
							// log.Printf("node %d  rpc error get vote from %d",rf.me,index)
							votecount <- 1
						}
						// rf.commitemu.Lock()
						// log.Printf("node %v (%v) request node %v (%v)  %v",rf.me,rf.commitedindex,index,reply.MaxCommitedIndex,reply.VoteGranted)
						// rf.commitemu.Unlock()
					}(i, args, reply)
				}

				go func(votecount chan int, candidateover chan int) {
					sum := 1
					n := len(rf.peers) - 1
					flag := false
					flag2 := false
					maxCindex := 0
					for x := range votecount {
						// log.Printf("node %d calculate vote : %d",rf.me,x)
						if x == 2 {
							sum++
						} else {

							maxCindex = max(maxCindex, -x)
							if !flag2 {
								rf.commitemu.Lock()
								if maxCindex > rf.commitedindex {
									flag2 = true
								}
								rf.commitemu.Unlock()

								if flag2 {
									candidateover <- -maxCindex
								}
							}
						}
						if !flag {
							if sum >= (len(rf.peers)/2 + 1) {
								candidateover <- 2
								flag = true
							}
						}
						// log.Printf("node %d calculate vote over : %d",rf.me,x)
						n--
						if n == 0 {
							close(votecount)
						}
					}
					// log.Printf("node %d term %d candidateover",rf.me,)
					if !flag && !flag2 {
						candidateover <- -maxCindex
					}
				}(votecount, candidateover)

			retry := true
			for retry {
				retry = false
				select {
				case x := <-candidateover:
					// log.Printf("node %d candidateover result:%d",rf.me,x)
					if x == 2 {
						rf.commitemu.Lock()
						// log.Printf("term:%d node(candidate) %d became leader  commitedindex:%v", rf.term, rf.me, rf.commitedindex)
						rf.commitemu.Unlock()
						rf.statusmu.Lock()
						rf.status = 2
						rf.statusmu.Unlock()
					} else {
						maxindex := -x
						rf.commitemu.Lock()
						// log.Printf("node %d(%d) get maxindex: %d",rf.me,rf.commitedindex,maxindex)
						if maxindex > rf.commitedindex {
							rf.statusmu.Lock()
							rf.status = 0
							rf.statusmu.Unlock()
							rf.maxcommitedIndex = max(rf.maxcommitedIndex, maxindex)
							rf.commitemu.Unlock()
						} else {
							rf.commitemu.Unlock()
						}
					}
				case <-time.After(1000 * time.Millisecond):
					// log.Printf("node %v timeout",rf.me)
					time.Sleep(time.Duration(rand.Intn(700)) * time.Millisecond)
					continue
				case term := <-rf.heartbeat:
					// log.Printf("node %v get heartbeat",rf.me)
					rf.termmu.Lock()
					if term > rf.term {
						rf.termmu.Unlock()
						// log.Printf("term:%d node(candidate) %d become follower",rf.term,rf.me)
						rf.term = term
						rf.statusmu.Lock()
						rf.status = 0
						rf.statusmu.Unlock()
					} else {
						rf.termmu.Unlock()
						// log.Printf("node %d ignoreCandidate once",rf.me)
						// ignoreCandidate = true
						retry = true
					}

				case term := <-rf.getvoterequest:
					rf.termmu.Lock()
					if term > rf.term {
						// log.Printf("getvoterequest: term:%d>%d node(candidate) %d become follower",term,rf.term,rf.me)
						rf.term = term
						rf.termmu.Unlock()

						rf.statusmu.Lock()
						rf.status = 0
						rf.statusmu.Unlock()

					} else {
						rf.termmu.Unlock()
						// ignoreCandidate = true
						retry = true
					}
				case <-rf.taskCh:
					continue
				}
			}
		} else if rf.status == 2 { //leader
			rf.statusmu.Unlock()
				rf.termmu.Lock()
				currentterm := rf.term
				rf.termmu.Unlock()
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					go func(index int) {
						flag := true
						rf.statusmu.Lock()
						if rf.status != 2 {
							flag = false
						}
						rf.statusmu.Unlock()
						if !flag {
							return
						}
						rf.cmdmu.Lock()
						rf.commitemu.Lock()
						cmd := CmdEntry{Index: -1}
						if rf.commitedindex >= 0 {
							cmd = CmdEntry{Index: rf.cmdarray[rf.commitedindex].Index, Term: rf.cmdarray[rf.commitedindex].Term}
						}
						args := AppendEntriesArgs{Term: currentterm, Empty: true, Commitedindex: rf.commitedindex, LeaderID: rf.me, CurrentCmd: cmd}
						rf.commitemu.Unlock()
						rf.cmdmu.Unlock()
						if args.CurrentCmd.Index > 0 {
							args.LastCmdIndex = rf.cmdarray[args.CurrentCmd.Index-1].Index
							args.LastCmdTerm = rf.cmdarray[args.CurrentCmd.Index-1].Term
						}
						reply := AppendEntriesReply{}
						ok := false
						try := 0
						for !ok {
							try++
							ok = rf.sendAppendEntries(index, &args, &reply)
							if ok {
								break
							}
							time.Sleep(400 * time.Millisecond)
							flag := true
							rf.statusmu.Lock()
							if rf.status != 2 {
								flag = false
							}
							rf.statusmu.Unlock()
							if !flag {
								return
							}
						}
						// logcount++
						// log.Printf("%d: heartbeat: node %d apply to node %d  current: %d try: %d",logcount, rf.me, index, cmd.Index, try)
						// ok := false
						// ok = rf.sendAppendEntries(index, &args, &reply)
						// if (!ok) {
						// 	return
						// }
						// log.Printf("send empty apE")
						if reply.Accept {
							//
						} else {
							if reply.Term > currentterm {
								rf.heartbeat <- reply.Term
								return
							}
							lasted := args.CurrentCmd.Index

							currentIndex := lasted

							if reply.ConflictTerm == 0 {
								currentIndex = reply.ConflictIndex
							} else {
								flag := false
								rf.cmdmu.Lock()
								for i := currentIndex; i >= 0; i-- {
									if rf.cmdarray[i].Term == reply.ConflictTerm {
										flag = true
										currentIndex = i + 1
										break
									}
								}
								if !flag {
									currentIndex = reply.ConflictIndex
								}
								rf.cmdmu.Unlock()
							}
							args.Empty = false
							for currentIndex <= lasted {
								// log.Printf("heartbeat: node %d apply to node %d  current: %d to lasted: %d",rf.me,index,currentIndex,lasted)
								flag := true
								rf.statusmu.Lock()
								if rf.status != 2 {
									flag = false
								}
								rf.statusmu.Unlock()
								if !flag {
									return
								}
								rf.cmdmu.Lock()
								args.CurrentCmd = rf.cmdarray[currentIndex]
								if currentIndex > 0 {
									args.LastCmdIndex = rf.cmdarray[currentIndex-1].Index
									args.LastCmdTerm = rf.cmdarray[currentIndex-1].Term
								}
								rf.cmdmu.Unlock()
								reply = AppendEntriesReply{}
								ok := false
								try := 0
								for !ok {
									try++
									ok = rf.sendAppendEntries(index, &args, &reply)
									if ok {
										break
									}
									time.Sleep(400 * time.Millisecond)
									flag := true
									rf.statusmu.Lock()
									if rf.status != 2 {
										flag = false
									}
									rf.statusmu.Unlock()
									if !flag {
										return
									}
								}
								// logcount++
								// log.Printf("%d : heartbeat: node %d apply to node %d  current: %d to lasted: %d  try: %d",logcount, rf.me, index, currentIndex, lasted, try)
								// ok := false
								// ok = rf.sendAppendEntries(index, &args, &reply)
								// if (!ok) {
								// 	return
								// }
								if reply.Accept {
									currentIndex = max(currentIndex, reply.ConflictIndex) + 1
								} else {
									if reply.Term > currentterm {
										rf.heartbeat <- reply.Term
										return
									}
									if reply.ConflictTerm == 0 {
										currentIndex = reply.ConflictIndex
									} else {
										flag := false
										rf.cmdmu.Lock()
										for i := currentIndex; i >= 0; i-- {
											if rf.cmdarray[i].Term == reply.ConflictTerm {
												flag = true
												currentIndex = i + 1
												break
											}
										}
										rf.cmdmu.Unlock()
										if !flag {
											currentIndex = reply.ConflictIndex
										}

									}
								}
							}
						}
					}(i)
				}

			retry:=true
			for retry {
				retry = false
			select {
			case index := <-rf.commiteCh:
				rf.commitemu.Lock()
				lastcommited := rf.commitedindex
				rf.commitedindex = max(rf.commitedindex, index)
				newcommited := rf.commitedindex
				rf.commitemu.Unlock()
				// log.Printf("node %d commite from %d to %d",rf.me,lastcommited+1,newcommited)
				for i := lastcommited + 1; i <= newcommited; i++ {
					rf.cmdmu.Lock()
					applymsg := ApplyMsg{CommandValid: true, Command: rf.cmdarray[i].Command, CommandIndex: i + 1}
					rf.cmdmu.Unlock()
					// log.Printf("node %d apply : %v",rf.me,applymsg)
					rf.applyCh <- applymsg
				}
			case <-time.After(100 * time.Millisecond):
				// log.Printf("leader timeout")
				rf.realtaskcount = 0
				rf.cmdmu.Lock()
				index := len(rf.cmdarray) - 1
				rf.cmdmu.Unlock()
				rf.commitemu.Lock()
				if index > rf.commitedindex {
					go func() {
						rf.realtaskCh <- index
					}()
					retry = true
				}
				rf.commitemu.Unlock()
			case <-rf.taskCh:
				rf.realtaskcount++
				if rf.realtaskcount == 10 {
					rf.realtaskcount = 0
					rf.cmdmu.Lock()
					index := len(rf.cmdarray) - 1
					rf.cmdmu.Unlock()
					go func() {
						rf.realtaskCh <- index
					}()
				}
				retry = true
			case index := <-rf.realtaskCh:
				rf.commitemu.Lock()
				if index <= rf.commitedindex {
					// log.Printf("ignore a task")
					rf.commitemu.Unlock()
					// leaderflag = true
					continue
				}
				rf.commitemu.Unlock()
				rf.cmdmu.Lock()
				cmd := rf.cmdarray[index]
				rf.cmdmu.Unlock()
				term := cmd.Term
				Ch := make(chan int)
				// log.Printf("---------------get taskCh %v----------",cmd.Index)
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					go func(i, index int) {
						flag := true
						rf.statusmu.Lock()
						if rf.status != 2 {
							flag = false
						}
						rf.statusmu.Unlock()
						if !flag {
							Ch <- 0
							return
						}
						rf.commitemu.Lock()
						args := AppendEntriesArgs{Term: term, CurrentCmd: cmd, Empty: false, LeaderID: rf.me, Commitedindex: rf.commitedindex}
						rf.commitemu.Unlock()
						reply := AppendEntriesReply{}
						if index > 0 {
							rf.cmdmu.Lock()
							args.LastCmdIndex = rf.cmdarray[index-1].Index
							args.LastCmdTerm = rf.cmdarray[index-1].Term
							rf.cmdmu.Unlock()
						}
						ok := false
						try := 0
						for !ok {
							try++
							ok = rf.sendAppendEntries(i, &args, &reply)
							if ok {
								break
							}
							time.Sleep(400 * time.Millisecond)
							flag := true
							rf.statusmu.Lock()
							if rf.status != 2 {
								flag = false
							}
							rf.statusmu.Unlock()
							if !flag {
								Ch <- 0
								return
							}
						}
						// logcount++
						// log.Printf("%d : task: node %d apply to node %d  current: %d try: %d",logcount, rf.me, i, index, try)
						// ok := false
						// ok = rf.sendAppendEntries(i, &args, &reply)
						// if (!ok) {
						// 	Ch<-0
						// 	return
						// }
						if reply.Accept {
							Ch <- 1
						} else {
							if reply.Term > term {
								rf.heartbeat <- reply.Term
								Ch <- 0
								return
							}
							currentIndex := index
							if reply.ConflictTerm == 0 {
								currentIndex = reply.ConflictIndex
							} else {
								flag := false
								rf.cmdmu.Lock()
								for i := currentIndex; i >= 0; i-- {
									if rf.cmdarray[i].Term == reply.ConflictTerm {
										flag = true
										currentIndex = i + 1
										break
									}
								}
								if !flag {
									currentIndex = reply.ConflictIndex
								}
								rf.cmdmu.Unlock()
							}

							for currentIndex <= index {
								flag := true
								rf.statusmu.Lock()
								if rf.status != 2 {
									flag = false
								}
								rf.statusmu.Unlock()
								rf.commitemu.Lock()
								if currentIndex < rf.commitedindex {
									flag = false
								}
								rf.commitemu.Unlock()
								if !flag {
									Ch <- 0
									return
								}
								rf.cmdmu.Lock()
								args.CurrentCmd = rf.cmdarray[currentIndex]
								if currentIndex > 0 {
									args.LastCmdIndex = rf.cmdarray[currentIndex-1].Index
									args.LastCmdTerm = rf.cmdarray[currentIndex-1].Term
								}
								rf.cmdmu.Unlock()
								reply = AppendEntriesReply{}
								ok := false
								try := 0
								for !ok {
									try++
									ok = rf.sendAppendEntries(i, &args, &reply)
									if ok {
										break
									}
									time.Sleep(400 * time.Millisecond)
									flag := true
									rf.statusmu.Lock()
									if rf.status != 2 {
										flag = false
									}
									rf.statusmu.Unlock()
									if !flag {
										Ch <- 0
										return
									}
								}
								// logcount++
								// log.Printf("%d : task: node %d apply to node %d  current: %d to lasted: %d  try: %d",logcount, rf.me, i, currentIndex, index, try)
								// ok := false
								// ok = rf.sendAppendEntries(i, &args, &reply)
								// if (!ok) {
								// 	Ch<-0
								// 	return
								// }
								if reply.Accept {
									currentIndex = max(currentIndex, reply.ConflictIndex) + 1
								} else {
									if reply.Term > term {
										rf.heartbeat <- reply.Term
										Ch <- 0
										return
									}
									if reply.ConflictTerm == 0 {
										currentIndex = reply.ConflictIndex
									} else {
										flag := false
										rf.cmdmu.Lock()
										for i := currentIndex; i >= 0; i-- {
											if rf.cmdarray[i].Term == reply.ConflictTerm {
												flag = true
												currentIndex = i + 1
												break
											}
										}
										if !flag {
											currentIndex = reply.ConflictIndex
										}
										rf.cmdmu.Unlock()
									}
								}
							}
							Ch <- 1
						}
					}(i, index)
				}

				n := len(rf.peers) - 1
				sum := 1
				go func(index int) {
					flag := false
					for x := range Ch {
						sum += x
						n--
						if !flag && sum >= (len(rf.peers)/2+1) {
							flag = true
							rf.commiteCh <- index
						}
						if n == 0 {
							close(Ch)
						}
					}

				}(index)
				retry = true
				// time.Sleep(20*time.Millisecond)
			case term := <-rf.heartbeat:
				rf.termmu.Lock()
				if term > rf.term {
					// log.Printf("getheartbeat: term:%d>%d node(leader) %d become follower",term,rf.term,rf.me)
					rf.term = term

					rf.statusmu.Lock()
					rf.status = 0
					rf.statusmu.Unlock()
				}else{
					retry = true
				}
				rf.termmu.Unlock()
			case term := <-rf.getvoterequest:
				rf.termmu.Lock()
				if term > rf.term {
					// log.Printf("getvoterequest: term:%d>%d node(leader) %d become follower",term,rf.term,rf.me)
					rf.term = term

					rf.statusmu.Lock()
					rf.status = 0
					rf.statusmu.Unlock()
					rf.termmu.Unlock()
				}else{
					retry = true
				}

			}
		}

		}

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.term = 1
	rf.status = 0
	rf.votemaxterm = 0
	rf.heartbeat = make(chan int, 10)
	rf.getvoterequest = make(chan int, 10)

	rf.commiteCh = make(chan int, 10)
	rf.cmdarray = make([]CmdEntry, 0)
	rf.commitedindex = -1
	rf.maxcommitedIndex = -2
	rf.taskCh = make(chan int, 10)
	rf.realtaskCh = make(chan int, 10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
