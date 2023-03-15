package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	rwMutex  sync.RWMutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// Given the identifier (hash), returns the block
	// Acquire read lock
	bs.rwMutex.RLock()
	block := bs.BlockMap[blockHash.Hash]
	bs.rwMutex.RUnlock()
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// Compute hash, and add the block to the map
	hash := GetBlockHashString(block.BlockData)
	// Acquire write lock
	bs.rwMutex.Lock()
	bs.BlockMap[hash] = block
	// Acquire write lock
	bs.rwMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	outHashes := make([]string, 0)
	// Iterate over the input hashes and check if it exists in the map or not
	for _, blockHash := range blockHashesIn.Hashes {
		// Acquire read lock
		bs.rwMutex.RLock()
		_, exists := bs.BlockMap[blockHash]
		bs.rwMutex.RUnlock()
		if exists {
			outHashes = append(outHashes, blockHash)
		}
	}
	return &BlockHashes{Hashes: outHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var blockHashes *BlockHashes = &BlockHashes{}
	var hashes []string
	for hash, _ := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	blockHashes.Hashes = hashes
	return blockHashes, nil
}
