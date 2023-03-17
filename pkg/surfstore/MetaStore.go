package surfstore

import (
	context "context"
	// "log"
	// "fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	rwMutex            sync.RWMutex
	UnimplementedMetaStoreServer
}

// Returns mapping of files and its metadata (version, filename and hashlist)
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// Aqcuire read lock
	m.rwMutex.RLock()
	var fileInfoMap *FileInfoMap = &FileInfoMap{FileInfoMap: m.FileMetaMap}
	m.rwMutex.RUnlock()
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	fileVersion := fileMetaData.Version
	// Acquire read lock
	m.rwMutex.RLock()
	_, exists := m.FileMetaMap[fileName]
	m.rwMutex.RUnlock()
	if exists {
		m.rwMutex.RLock()
		curFileVersion := m.FileMetaMap[fileName].Version
		m.rwMutex.RUnlock()
		// Replace the metadata only if the version is 1 greater than current file version
		if fileVersion == 1+curFileVersion {
			m.rwMutex.Lock()
			m.FileMetaMap[fileName] = fileMetaData
			m.rwMutex.Unlock()
		} else {
			// Else send version -1 to the client
			return &Version{Version: -1}, nil
		}
	} else {
		m.rwMutex.Lock()
		m.FileMetaMap[fileName] = fileMetaData
		m.rwMutex.Unlock()
	}
	return &Version{Version: fileVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var blockStoreMap *BlockStoreMap = &BlockStoreMap{}
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)
	var blockHashes map[string]*BlockHashes = make(map[string]*BlockHashes)
	for _, blockHashIn := range blockHashesIn.Hashes {
		m.rwMutex.RLock()
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHashIn)
		m.rwMutex.RUnlock()
		_, exists := blockHashes[responsibleServer]
		if exists {
			// entry.Hashes = append(entry.Hashes, blockHashIn)
			blockHashes[responsibleServer].Hashes = append(blockHashes[responsibleServer].Hashes, blockHashIn)
		} else {
			blockHashes[responsibleServer] = &BlockHashes{Hashes: []string{blockHashIn}}
		}
	}
	// log.Println("blockHashes", blockHashes)
	for server, hashList := range blockHashes {
		_, exists := blockStoreMap.BlockStoreMap[server]
		if exists {
			blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, hashList.Hashes...)
		} else {
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{Hashes: hashList.Hashes}
		}
	}
	// blockStoreMap.BlockStoreMap = blockHashes
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	m.rwMutex.RLock()
	var blockStoreAddrs *BlockStoreAddrs = &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}
	m.rwMutex.RUnlock()
	return blockStoreAddrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
