package SurfTest

import (
	"fmt"
	"os"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
	// "log"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	// // fmt.println("states of servers")
	test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	// // fmt.println("0 crashed")
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	// // fmt.println("1 leader set and send heartbeat done")
	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestSyncTwoClientsClusterFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. majority of the cluster crashes. client2 syncs again.")
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	// 0 leader
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	fmt.Println("Client 1 sync done")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	// // // fmt.println("states of servers")
	// test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	// test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})

	// test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	// // // fmt.println("0 crashed")
	// test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	// test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	// // fmt.println("1 leader set and send heartbeat done")
	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	fmt.Println("Client 2 sync done")
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// Majority fails
	// test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[3].Crash(test.Context, &emptypb.Empty{})
	test.Clients[4].Crash(test.Context, &emptypb.Empty{})
	fmt.Println("All crash")

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	// fmt.println("Client 1 sync done")
}

// leader1 gets a request while the majority of the cluster is down.
// leader1 crashes.
// the other nodes come back.
// leader2 is elected
func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	t.Logf("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// 2, 3, 4 crash (majority)
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[3].Crash(test.Context, &emptypb.Empty{})
	test.Clients[4].Crash(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	// err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	hashList := []string{"a", "b"}
	fileMetaData := NewFileMetaDataFromParams("a.txt", 2, hashList)
	// version, err := test.Clients[0].UpdateFile(test.Context, fileMetaData)
	go test.Clients[0].UpdateFile(test.Context, fileMetaData)
	time.Sleep(time.Second*5)
	// if err != nil {
	// 	// fmt.Println("updatefile err", err, version)
	// 	// t.Fatalf("UpdateFile failed")
	// }
	// fmt.Println("Update file done")

	// prevstate0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	// fmt.Println("Server 0 state")
	// fmt.Println("isLeader", prevstate0.IsLeader)
	// fmt.Println("Term", prevstate0.Term)
	// fmt.Println("Log", prevstate0.Log)
	// fmt.Println("MetaMap", prevstate0.MetaMap.FileInfoMap)

	// prevstate1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})

	// fmt.Println("Server 1 state")
	// fmt.Println("isLeader", prevstate1.IsLeader)
	// fmt.Println("Term", prevstate1.Term)
	// fmt.Println("Log", prevstate1.Log)
	// fmt.Println("MetaMap", prevstate1.MetaMap.FileInfoMap)

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	// fmt.Println("0 crashed")

	// other nodes (2,3,4) restore
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[3].Restore(test.Context, &emptypb.Empty{})
	test.Clients[4].Restore(test.Context, &emptypb.Empty{})

	// leader 2 is elected
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// workingDir, _ := os.Getwd()

	// fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	// fmt.Println("client1 fileMeta", fileMeta1)
	state0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	fmt.Println("Server 0 state")
	fmt.Println("isLeader", state0.IsLeader)
	fmt.Println("Term", state0.Term)
	fmt.Println("Log", state0.Log)
	fmt.Println("MetaMap", state0.MetaMap.FileInfoMap)

	state1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})

	fmt.Println("Server 1 state")
	fmt.Println("isLeader", state1.IsLeader)
	fmt.Println("Term", state1.Term)
	fmt.Println("Log", state1.Log)
	fmt.Println("MetaMap", state1.MetaMap.FileInfoMap)

	state2, _ := test.Clients[2].GetInternalState(test.Context, &emptypb.Empty{})

	fmt.Println("Server 2 state")
	fmt.Println("isLeader", state2.IsLeader)
	fmt.Println("Term", state2.Term)
	fmt.Println("Log", state2.Log)
	fmt.Println("MetaMap", state2.MetaMap.FileInfoMap)

	state3, _ := test.Clients[3].GetInternalState(test.Context, &emptypb.Empty{})

	fmt.Println("Server 3 state")
	fmt.Println("isLeader", state3.IsLeader)
	fmt.Println("Term", state3.Term)
	fmt.Println("Log", state3.Log)
	fmt.Println("MetaMap", state3.MetaMap.FileInfoMap)

	state4, _ := test.Clients[4].GetInternalState(test.Context, &emptypb.Empty{})

	fmt.Println("Server 4 state")
	fmt.Println("isLeader", state4.IsLeader)
	fmt.Println("Term", state4.Term)
	fmt.Println("Log", state4.Log)
	fmt.Println("MetaMap", state4.MetaMap.FileInfoMap)
}
