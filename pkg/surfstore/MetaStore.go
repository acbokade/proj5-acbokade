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
	// log.Println("server updateFile 1")
	m.rwMutex.RLock()
	_, exists := m.FileMetaMap[fileName]
	m.rwMutex.RUnlock()
	// log.Println("server updateFile 2")
	if exists {
		m.rwMutex.RLock()
		curFileVersion := m.FileMetaMap[fileName].Version
		m.rwMutex.RUnlock()
		// log.Println("server updateFile 3")
		// Replace the metadata only if the version is 1 greater than current file version
		if fileVersion == 1+curFileVersion {
			m.rwMutex.Lock()
			m.FileMetaMap[fileName] = fileMetaData
			m.rwMutex.Unlock()
		} else {
			// Else send version -1 to the client
			return &Version{Version: -1}, nil
		}
		// log.Println("server updateFile 4")
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
	// // fmt.println("GetBlockStoreMap blockHashesIn", blockHashesIn)
	// log.Println("Metastore blockHashesIn length", len(blockHashesIn.Hashes))
	for _, blockHashIn := range blockHashesIn.Hashes {
		m.rwMutex.RLock()
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHashIn)
		// // fmt.println("GetBlockStoreMap responsibleServer", responsibleServer)
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
		// log.Println("**", server, exists)
		if exists {
			blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, hashList.Hashes...)
		} else {
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{Hashes: hashList.Hashes}
		}
	}
	// log.Println("blockStoreMap", blockStoreMap.BlockStoreMap)
	// blockStoreMap.BlockStoreMap = blockHashes
	// // fmt.println("GetBlockStoreMap blockStoreMap", blockStoreMap)
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// log.Println("metastore BlockStoreAddrs", m.BlockStoreAddrs)
	m.rwMutex.RLock()
	var blockStoreAddrs *BlockStoreAddrs = &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}
	m.rwMutex.RUnlock()
	// log.Println("metastore blockStoreAddrs", blockStoreAddrs)
	return blockStoreAddrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	// log.Println("meta store ctr BlockStoreAddrs", blockStoreAddrs)
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
