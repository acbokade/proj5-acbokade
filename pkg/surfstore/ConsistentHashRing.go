package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for serverHash := range c.ServerMap {
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)
	var responsibleServerAddr = ""
	blockHash := blockId
	firstServerAddr := ""
	for _, serverHash := range hashes {
		if len(firstServerAddr) == 0 {
			firstServerAddr = c.ServerMap[serverHash]
		}
		if serverHash > blockHash {
			responsibleServerAddr = c.ServerMap[serverHash]
			break
		}
	}
	if len(responsibleServerAddr) == 0 {
		responsibleServerAddr = firstServerAddr
	}
	return responsibleServerAddr
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	var serverMap map[string]string = make(map[string]string)
	var consistentHashRing *ConsistentHashRing = &ConsistentHashRing{}
	for _, serverAddr := range serverAddrs {
		blockStoreAddr := "blockstore" + serverAddr
		serverHash := consistentHashRing.Hash(blockStoreAddr)
		serverMap[serverHash] = serverAddr
	}
	consistentHashRing.ServerMap = serverMap
	return consistentHashRing
}
