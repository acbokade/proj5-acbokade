package surfstore

import (
	context "context"
	// "fmt"
	"sync"
	// "errors"
	// "fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"math"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore      *MetaStore
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Check crash
	// fmt.Println("server GetFileInfoMap", s.isCrashed, s.isLeader)
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// If not a leader, return error
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	// Check if majority of nodes are active, if not, block
	for {
		if s.checkMajorityOfNodesActive() {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Check crash
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// If not a leader, return error
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	// Check if majority of nodes are active, if not, block
	for {
		if s.checkMajorityOfNodesActive() {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Check crash
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// If not a leader, return error
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	// Check if majority of nodes are active, if not, block
	for {
		if s.checkMajorityOfNodesActive() {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Check crash
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// If not a leader, return error
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	// fmt.Println("logic of updateFile")

	// Append entry to the local log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	// Send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)
	// Keep trying indefinitely (even after responding) ** rely on sendhearbeat
	// Commit the entry once majority of followers have it in their log
	commit := <-commitChan
	// fmt.Println("received commit from commitChan")
	// Once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// Send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	// Contact all followers and send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, addr, responses)
	}
	totalResponses := 1
	totalAppends := 1
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
		// fmt.Println("total appends responses", totalAppends, totalResponses)
	}
	if totalAppends > len(s.peers)/2 {
		*s.pendingCommits[s.commitIndex] <- true
		s.commitIndex++
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, response chan bool) {
	var prevLogTerm int64 = 0
	var prevLogIndex int64 = -1
	// fmt.Println("log", s.log)
	if s.commitIndex > 0 && len(s.log) >= 2 {
		prevLogTerm = s.log[s.commitIndex-1].Term
		prevLogIndex = s.commitIndex - 1
	}
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	var conn *grpc.ClientConn
	var err error
	conn, err = grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		response <- false
		return
	}
	client := NewRaftSurfstoreClient(conn)
	appendEntryOutput, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
	// outputTerm := appendEntryOutput.Term
	if err == nil {
		// fmt.Println("appendEntryOutput", appendEntryOutput)
		outputSuccess := appendEntryOutput.Success
		if outputSuccess {
			// TODO check output of append entries
			response <- true
		} else {
			response <- false
		}
	} else {
		response <- false
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// Check crash
	// fmt.Println("append entry entered")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		// fmt.Println("crashed server")
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	leaderTerm := input.Term
	prevLogIndex := input.PrevLogIndex
	prevLogTerm := input.PrevLogTerm
	entries := input.Entries
	leaderCommitIndex := input.LeaderCommit
	// TODO: actually check entries (Term can differ)
	// fmt.Println("leaderTerm, sterm sid", leaderTerm, s.term, s.id)
	if leaderTerm > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = leaderTerm
	}
	// Case 1
	if leaderTerm < s.term {
		// False response
		// fmt.Println("case 1")
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	// Case 2
	if int64(len(s.log)) <= prevLogIndex || (prevLogIndex != -1 && s.log[prevLogIndex].Term != prevLogTerm) {
		// False response (?: The client would send another RPC with prevLogIndex - 1)
		// fmt.Println("case 2")
		// var term int64 = 0
		// if prevLogIndex < int64(len(s.log)) {
		// 	term = s.log[prevLogIndex].Term
		// }
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	// fmt.Println("case 3 onwards")
	// Case 3
	updateEntries := make([]*UpdateOperation, 0)
	for idx, entry := range s.log {
		s.commitIndex = int64(idx) - 1 //?
		if idx >= len(entries) {
			// delete remaining
			s.log = s.log[:idx]
			break
		}
		if !SameOperation(entry, entries[idx]) || entry.Term != entries[idx].Term {
			s.log = s.log[:idx]
			updateEntries = entries[idx:]
			break
		}
		// Last index reached
		if idx == len(s.log)-1 {
			updateEntries = entries[idx+1:]
		}
	}
	// fmt.Println("case 4 onwards")
	// Case 4
	s.log = append(s.log, updateEntries...)

	// fmt.Println("case 5 onwards")
	// Case 5
	if s.commitIndex < leaderCommitIndex {
		s.commitIndex = int64(math.Min(float64(leaderCommitIndex), float64(len(s.log)-1)))
	}
	// Term can also differ
	for s.lastApplied+1 <= s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	// fmt.Println("last case appendentry")
	return &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: -1,
	}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Check crash
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term++
	// TODO update state
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Check crash
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// Check leader
	// If not a leader, do nothing
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: true}, nil
	}
	s.isLeaderMutex.RUnlock()

	var prevLogTerm int64 = 0
	var prevLogIndex int64 = s.commitIndex - 1
	if prevLogIndex != -1 && prevLogIndex <= int64(len(s.log)-1) {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	// Contact all followers and send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		// fmt.Println("s.id", s.id, "sent ", dummyAppendEntriesInput.Term, "to", idx)
		appendEntryOutput, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		// otherServerId := appendEntryOutput.ServerId
		// fmt.Println("appendEntryOutput", appendEntryOutput)
		if appendEntryOutput != nil {
			otherTerm := appendEntryOutput.Term
			// otherSuccess := appendEntryOutput.Success
			// otherMatchedIndex := appendEntryOutput.MatchedIndex
			if otherTerm > s.term {
				// Step down as leader
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
			}
		}
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) checkMajorityOfNodesActive() bool {
	nServers := len(s.peers)
	majority := int64((nServers + 1) / 2)
	active := 0
	for _, addr := range s.peers {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		active++
		conn.Close()
	}
	return active >= int(majority)
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

func SameOperation(op1, op2 *UpdateOperation) bool {
	if op1 == nil && op2 == nil {
		return true
	}
	if op1 == nil || op2 == nil {
		return false
	}
	if op1.Term != op2.Term {
		return false
	}
	if op1.FileMetaData == nil && op2.FileMetaData != nil ||
		op1.FileMetaData != nil && op2.FileMetaData == nil {
		return false
	}
	if op1.FileMetaData.Version != op2.FileMetaData.Version {
		return false
	}
	if !SameHashList(op1.FileMetaData.BlockHashList, op2.FileMetaData.BlockHashList) {
		return false
	}
	return true
}

func SameHashList(list1, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}

	for i := 0; i < len(list1); i++ {
		if list1[i] != list2[i] {
			return false
		}
	}

	return true
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
