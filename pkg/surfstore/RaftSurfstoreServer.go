package surfstore

import (
	context "context"
	// "errors"
	// "fmt"
	"strings"
	"sync"

	// "fmt"
	"math"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
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
		if s.checkMajorityOfNodesActive(ctx) {
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
		if s.checkMajorityOfNodesActive(ctx) {
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
		if s.checkMajorityOfNodesActive(ctx) {
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
	// Once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	// return s.metaStore.UpdateFile(ctx, filemeta)
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
		if totalAppends > len(s.peers)/2 {
			break
		}
		if totalResponses == len(s.peers) {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		s.commitIndex++
		*s.pendingCommits[len(s.pendingCommits)-1] <- true
	} else {
		*s.pendingCommits[len(s.pendingCommits)-1] <- false
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, response chan bool) {
	var prevLogTerm int64 = 0
	var prevLogIndex int64 = -1
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
	var appendEntryOutput *AppendEntryOutput
	for {
		appendEntryOutput, err = client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if err == nil {
			break
		}
		// Check crash
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			response <- false
			s.isCrashedMutex.RUnlock()
			return
		}
		s.isCrashedMutex.RUnlock()
	}
	if err == nil {
		outputSuccess := appendEntryOutput.Success
		if outputSuccess {
			response <- true
		} else {
			response <- false
		}
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
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	// Return if other server is calling appendEntry for active status (Term = -100)
	if input.Term == int64(-100) {
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	leaderTerm := input.Term
	prevLogIndex := input.PrevLogIndex
	prevLogTerm := input.PrevLogTerm
	entries := input.Entries
	leaderCommitIndex := input.LeaderCommit
	if leaderTerm > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = leaderTerm
	}
	// Case 1
	if leaderTerm < s.term {
		// False response
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	// Case 2
	if prevLogIndex >= 0 && len(s.log) >= 1 && prevLogIndex < int64(len(s.log)) && s.log[prevLogIndex].Term != prevLogTerm {
		// False response
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	// Case 3
	updateEntries := make([]*UpdateOperation, 0)
	for idx, entry := range s.log {
		if idx >= len(entries) {
			// Delete after conflicting
			s.log = s.log[:idx]
			break
		}
		// Delete after conflicting
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
	if len(s.log) == 0 {
		updateEntries = entries[:]
	}
	// Case 4
	// Append any new entries
	s.log = append(s.log, updateEntries...)

	// Case 5
	if s.commitIndex < leaderCommitIndex {
		s.commitIndex = int64(math.Min(float64(leaderCommitIndex), float64(len(s.log))))
	}
	// Term can also differ
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
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
	var prevLogIndex int64 = -1
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
		appendEntryOutput, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if appendEntryOutput != nil {
			otherTerm := appendEntryOutput.Term
			if otherTerm > s.term {
				// Step down as leader
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				s.term = otherTerm
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

func (s *RaftSurfstore) checkMajorityOfNodesActive1(ctx context.Context, emptypb *emptypb.Empty) bool {
	nServers := len(s.peers)
	majority := int64((nServers + 1) / 2)
	active := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		isServerCrashed, err := client.GetCrashStatus(ctx, emptypb)
		if err == nil && !(*isServerCrashed).Flag {
			active++
		}
		conn.Close()
	}
	return active >= int(majority)
}

func (s *RaftSurfstore) checkMajorityOfNodesActive(ctx context.Context) bool {
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         int64(-100), // for dummy call
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	crashedCount := 0
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)
		_, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if err != nil && strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
			crashedCount++
		}
	}
	if crashedCount <= len(s.peers)/2 {
		return true
	}
	return false
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
