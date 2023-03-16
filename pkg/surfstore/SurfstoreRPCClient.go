package surfstore

import (
	context "context"
	"database/sql"
	"log"
	"os"
	"time"
	// "fmt"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := rpcClient.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := rpcClient.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retBlockHashes, err := rpcClient.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = append(*blockHashes, retBlockHashes.Hashes...)

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		rpcClient := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := rpcClient.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			// if err == ERR_SERVER_CRASHED{
			// 	return err
			// }
			continue
		}
		*serverFileInfoMap = fileInfoMap.FileInfoMap
		return nil
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		rpcClient := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version, err := rpcClient.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			// if err == ERR_SERVER_CRASHED{
			// 	return err
			// }
			continue
		}
		*latestVersion = version.Version
		return nil
	}
	return nil

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		rpcClient := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var blockHashes *BlockHashes = &BlockHashes{Hashes: blockHashesIn}
		retBlockStoreMap, err := rpcClient.GetBlockStoreMap(ctx, blockHashes)
		if err != nil {
			conn.Close()
			// if err == ERR_SERVER_CRASHED{
			// 	return err
			// }
			continue
		}
		(*blockStoreMap) = make(map[string][]string)
		for serverAddr, blockHashes := range retBlockStoreMap.BlockStoreMap {
			_, exists := (*blockStoreMap)[serverAddr]
			if exists {
				(*blockStoreMap)[serverAddr] = append((*blockStoreMap)[serverAddr], blockHashes.Hashes...)
			} else {
				(*blockStoreMap)[serverAddr] = append((*blockStoreMap)[serverAddr], blockHashes.Hashes...)
			}
		}
		return nil
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		// Crash
		if err != nil {
			continue
		}
		rpcClient := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		retBlockStoreAddr, err := rpcClient.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			// if err == ERR_SERVER_CRASHED{
			// 	return err
			// }
			continue
		}
		*blockStoreAddrs = append(*blockStoreAddrs, retBlockStoreAddr.BlockStoreAddrs...)
		return nil
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

func createIndexDbFile(path string) error {
	indexFile, e := os.Create(path)
	if e != nil {
		log.Fatal("Error During creating file", e)
		return e
	}
	// log.Println("index db created")
	indexFile.Close()
	db, err := sql.Open("sqlite3", path)
	defer db.Close()
	if err != nil {
		log.Fatal("Error during opening index.db file", err)
		return err
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During creating prepare statement for createTable", err)
	}
	_, err = statement.Exec()
	if err != nil {
		log.Fatal("Error while executing the statement", err)
		return err
	}
	statement.Close()
	// log.Println("table created")
	return nil
}

// func checkMajorityOfNodesActive(surfClient *RPCClient) bool {
// 	nServers := len(surfClient.MetaStoreAddrs)
// 	majority := int64((nServers + 1) / 2)
// 	active := 0
// 	for _, addr := range surfClient.MetaStoreAddrs {
// 		conn, err := grpc.Dial(addr, grpc.WithInsecure())
// 		if err != nil {
// 			continue
// 		}
// 		active++
// 		conn.Close()
// 	}
// 	return active >= int(majority)
// }

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); os.IsNotExist(err) {
		err := createIndexDbFile(outputMetaPath)
		if err != nil {
			log.Fatal("Error while creating index db file", err)
		}
	}
	// fmt.Println("created rpc client")
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}

// func connectToLeaderServer(surfClient* RPCClient) (*grpc.ClientConn, error) {
// 	metaStoreAddrs := surfClient.MetaStoreAddrs

// 	// heartbeat
// 	for _, server := range test.Clients {
// 		server.SendHeartbeat(test.Context, &emptypb.Empty{})
// 	}
// }

// func establishConnectionToAllServers(surfClient *RPCClient) ([]RaftSurfstoreClient, []*grpc.ClientConn) {
// 	clients := make([]RaftSurfstoreClient, 0)
// 	conns := make([]*grpc.ClientConn, 0)
// 	for _, addr := range surfClient.MetaStoreAddrs {
// 		conn, err := grpc.Dial(addr, grpc.WithInsecure())
// 		if err != nil {
// 			continue
// 		}
// 		client := NewRaftSurfstoreClient(conn)

// 		conns = append(conns, conn)
// 		clients = append(clients, client)
// 	}
// 	return clients, conns
// }

func connectToGrpcServer(storeAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(storeAddr, grpc.WithInsecure())
	return conn, err
}
