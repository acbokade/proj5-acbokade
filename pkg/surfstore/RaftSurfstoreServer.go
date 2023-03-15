package surfstore

import (
	context "context"
	// "fmt"
	"sync"
	// "errors"
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

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
	// Added for discussion
	id             int64
	leaderId       int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64
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
	return s.GetBlockStoreAddrs(ctx, empty)
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

	// Append entry to the log
	// Call append entry to all of the nodes
	// if yes from 4, commit entry (update metadata dict) and reply back to client

	// Append entry to the local log
	// Doubt: Concurrency?
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
	}
	if totalAppends > len(s.peers)/2 {
		// TODO put on correct channel
		*s.pendingCommits[len(s.log)-1] <- true
		// TODO update commit index correctly
		s.commitIndex = int64(len(s.log)) - 1
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, response chan bool) {
	var prevLogTerm int64 = 1
	var prevLogIndex int64 = 0
	if len(s.log) >= 2 {
		prevLogTerm = s.log[len(s.log)-2].Term
		prevLogIndex = int64(len(s.log) - 2)
	}
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// Put right values
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	// TODO check all errors
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			break
		}
	}
	client := NewRaftSurfstoreClient(conn)
	appendEntryOutput, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
	// outputTerm := appendEntryOutput.Term
	outputSuccess := appendEntryOutput.Success
	// outputServerId := appendEntryOutput.ServerId
	// outputMatchedIndex := appendEntryOutput.MatchedIndex
	if outputSuccess {
		// TODO check output of append entries
		response <- true
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
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	leaderTerm := input.Term
	prevLogIndex := input.PrevLogIndex
	prevLogTerm := input.PrevLogTerm
	entries := input.Entries
	leaderCommitIndex := input.LeaderCommit
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
	if int64(len(s.log)) <= prevLogIndex || s.log[prevLogIndex].Term != prevLogTerm {
		// False response (?: The client would send another RPC with prevLogIndex - 1)
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.log[prevLogIndex].Term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	// Case 3
	curIndex := prevLogIndex + 1
	var iter int64 = 0
	for {
		index := curIndex + iter
		if (int64(len(s.log))-1 >= index) && (s.log[index].Term != leaderTerm) {
			// Delete existing entry and all the other entries
			s.log = s.log[:index]
			break
		}
	}
	// Case 4
	iter = 0
	for {
		if len(entries) == 0 {
			break
		}
		index := curIndex + iter
		if int64(len(s.log)) >= index+1 {
			s.log[index] = entries[iter]
		} else {
			s.log = append(s.log, entries[iter])
		}
		iter++
		if iter >= int64(len(entries)) {
			break
		}
	}
	// TODO: actually check entries (Term can differ)
	if leaderTerm > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}
	// Case 5
	if s.commitIndex < leaderCommitIndex {
		s.commitIndex = int64(math.Min(float64(leaderCommitIndex), float64(len(s.log)-1)))
	}
	// s.log = input.Entries
	// Term can also differ
	for s.lastApplied <= s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	return nil, nil
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
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// put right values
		PrevLogTerm:  s.log[len(s.log)-1].Term,
		PrevLogIndex: int64(len(s.log) - 1),
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.lastApplied,
	}
	// Send initial empty AppendEntries call to all other peers
	for idx, peerAddr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, err := grpc.Dial(peerAddr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		_, _ = client.AppendEntries(ctx, &dummyAppendEntriesInput)
	}
	// TODO update state
	return nil, nil
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
	if s.isLeader {
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: true}, nil
	}
	s.isLeaderMutex.RUnlock()

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// Put right values
		PrevLogTerm:  s.log[len(s.log)-1].Term,
		PrevLogIndex: int64(len(s.log) - 1),
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.lastApplied,
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
		appendEntryOutput, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)

		// otherServerId := appendEntryOutput.ServerId
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

var _ RaftSurfstoreInterface = new(RaftSurfstore)
