package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	// "fmt"
)

// Returns number of blocks occupied by the file
func getNumberOfBlocks(fileSize int64, blockSize int) int {
	return int(math.Ceil(float64(fileSize) / float64(blockSize)))
}

func areEqualHashLists(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for idx, value := range first {
		if value != second[idx] {
			return false
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// log.Println("sync started")

	/*
		Download cases:
		1. When file is present locally, in local index and remote index, then if remote index version is more
		than local index version, download the file - COVERED
		2. When file is present locally, not in local index but present in remote index, then always download - COVERED
		3. If file is absent locally, present in both local and remote index, download it - COVERED
		4. If file is absent locally and in local index but present in remote index, download it. - COVERED
	*/

	/*
		Upload cases:
		1. When file is present locally, and absent in local and remote index - COVERED
	*/

	/*
		Delete cases:
		1. When file is absent locally but present in local and remote index, delete it if local index version
		equal remote index version.
	*/
	// Scan each file in the base directory and compute file's hash list.
	
	filesHashListMap := make(map[string][]string) // key - fileName, value - hashlist
	allFiles, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error while ReadDir call", err)
	}
	for _, file := range allFiles {
		// Ignore index.db file
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		fileName := file.Name()
		fileSize := file.Size()
		numBlocks := getNumberOfBlocks(fileSize, client.BlockSize)
		filePath := filepath.Join(client.BaseDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Println("Error opening file", err)
		}
		hashList := make([]string, 0)
		// Read each block and find its hash and append to hashList
		for i := 0; i < numBlocks; i++ {
			block := make([]byte, client.BlockSize)
			bytesRead, err := file.Read(block)
			if err != nil {
				log.Println("Error while reading from file ", err)
			}
			block = block[:bytesRead]
			blockHash := GetBlockHashString(block)
			hashList = append(hashList, blockHash)
		}
		file.Close()
		filesHashListMap[fileName] = hashList
	}
	// log.Println("filesHashListMap", filesHashListMap)

	// Load local index data from local db file
	// fmt.Println("to call loadmeta")
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error while loading metadata from database", err)
	}
	// log.Println("localIndex", localIndex)

	// Connect to server and download update FileInfoMap (remote index)
	var remoteIndex = make(map[string]*FileMetaData)
	// fmt.Println("to call fileInfoMap")
	err = client.GetFileInfoMap(&remoteIndex)
	if err == ERR_SERVER_CRASHED {
		log.Fatal("Server crashed")
	}
	// fmt.Println("done fileInfoMpa")
	// log.Println("remoteIndex", remoteIndex)

	// Files which are present in remoteIndex and not in localIndex needs to be downloaded
	filesToDownload := make(map[string]bool)
	filesToDelete := make(map[string]bool)
	filesToDeleteLocally := make(map[string]bool)
	// fmt.Println("remoteIndex", remoteIndex)
	for fileName := range remoteIndex {
		_, exists := localIndex[fileName]
		if !exists {
			filesToDownload[fileName] = true
		} else {
			// File exists in local but outdated version
			if remoteIndex[fileName].Version > localIndex[fileName].Version {
				// Check if file is deleted or not
				if len(remoteIndex[fileName].BlockHashList) == 1 && remoteIndex[fileName].BlockHashList[0] == TOMBSTONE_HASHVALUE {
					filesToDeleteLocally[fileName] = true
					deleteLocalFile(fileName, client, remoteIndex, localIndex)
				} else {
					filesToDownload[fileName] = true
				}
			}
			if remoteIndex[fileName].Version == localIndex[fileName].Version {
				// Check if file is already deleted
				if len(remoteIndex[fileName].BlockHashList) == 1 && remoteIndex[fileName].BlockHashList[0] == TOMBSTONE_HASHVALUE {
					continue
				}
				// If file doesnt exist locally but is in localIndex, download it
				_, exists := filesHashListMap[fileName]
				if !exists {
					filesToDelete[fileName] = true
				}
			}
		}
	}
	// fmt.Println(filesToDownload)
	// fmt.Println(filesToDelete)
	// fmt.Println(filesToDeleteLocally)
	// Get BlockStoreAddr
	var blockStoreAddrs []string
	_ = client.GetBlockStoreAddrs(&blockStoreAddrs)
	// if err == ERR_SERVER_CRASHED {
	// 	log.Fatal("Server crashed")
	// }
	// fmt.Println("client sync blockStoreAddrs", blockStoreAddrs)

	// Check the blocks to be downloaded
	for fileToDownload := range filesToDownload {
		downloadFile(fileToDownload, client, remoteIndex, localIndex, blockStoreAddrs)
	}

	// Check the blocks to be downloaded
	for fileToDeleteLocally := range filesToDeleteLocally {
		deleteLocalFile(fileToDeleteLocally, client, remoteIndex, localIndex)
	}

	// Check the files which are newly added or edited
	newFilesAdded := make([]string, 0)
	editedFiles := make([]string, 0)
	// fmt.Println("newFilesAdded", newFilesAdded)
	// fmt.Println("editedFiles", editedFiles)
	for fileName := range filesHashListMap {
		_, downloadExists := filesToDownload[fileName]
		_, deleteExists := filesToDelete[fileName]
		_, deleteLocallyExists := filesToDeleteLocally[fileName]
		if downloadExists || deleteExists || deleteLocallyExists {
			continue
		}
		_, exists := localIndex[fileName]
		_, remoteExists := remoteIndex[fileName]
		if !exists && !remoteExists {
			// File exists now in the local, but doesn't exist in the local index
			// Also, file doesn't exist in the remote index
			newFilesAdded = append(newFilesAdded, fileName)
		}
		if exists && remoteExists {
			// File exists in the local index, but has been changed since then
			// Version of the file in the local index and remote index are same
			if localIndex[fileName].Version == remoteIndex[fileName].Version {
				// Compare the hashList
				curHashList := filesHashListMap[fileName]
				prevHashList := localIndex[fileName].BlockHashList
				if !areEqualHashLists(curHashList, prevHashList) {
					editedFiles = append(editedFiles, fileName)
				}
			}
		}
	}
	// log.Println("filesToDownload", filesToDownload)
	// log.Println("newFilesAdded", newFilesAdded)
	// log.Println("editedFiles", editedFiles)
	// log.Println("filesToDelete", filesToDelete)
	// log.Println("filesToDeleteLocally", filesToDeleteLocally)

	// Check the blocks to be deleted
	for fileToDelete := range filesToDelete {
		deleteFile(fileToDelete, client, localIndex, blockStoreAddrs)
	}

	filesToUpload := make([]string, 0)
	filesToUpload = append(filesToUpload, newFilesAdded...)
	filesToUpload = append(filesToUpload, editedFiles...)
	// fmt.Println("filesToUpload", filesToUpload)
	// Upload newly added files
	for _, fileName := range filesToUpload {
		returnedVersion, err := uploadFile(fileName, client, localIndex, blockStoreAddrs)
		// log.Println("returnedVersion", returnedVersion)
		if err != nil || returnedVersion == -1 {
			// download only if it exists in remote index
			_, remoteExists := remoteIndex[fileName]
			if remoteExists {
				// outdated version
				downloadFile(fileName, client, remoteIndex, localIndex, blockStoreAddrs)
			}
		}
		// else {
		// 	// Only if update is successful, update the localIndex db
		// 	// WriteMetaFile(localIndex, client.BaseDir)
		// }
	}
	// log.Println("last localIndex", localIndex)
	WriteMetaFile(localIndex, client.BaseDir)
}

func uploadFile(fileName string, client RPCClient, localIndex map[string]*FileMetaData, blockStoreAddrs []string) (int32, error) {
	localPath := filepath.Join(client.BaseDir, fileName)
	localFile, err := os.Open(localPath)
	if err != nil {
		log.Println("Error while opening file", err)
	}
	defer localFile.Close()

	fileStats, err := os.Stat(localPath)
	if err != nil {
		log.Println("Error while fetching stats", err)
	}
	fileSize := fileStats.Size()
	numBlocks := getNumberOfBlocks(fileSize, client.BlockSize)
	hashList := make([]string, 0)
	blockHashToBlockDataMap := make(map[string][]byte)
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, client.BlockSize)
		bytesRead, err := localFile.Read(blockData)
		if err != nil {
			log.Println("Error while reading the file", err)
		}
		blockData = blockData[:bytesRead]
		blockHash := GetBlockHashString(blockData)
		hashList = append(hashList, blockHash)
		blockHashToBlockDataMap[blockHash] = blockData
	}
	// log.Println("upload hashlist length", len(hashList), len(blockHashToBlockDataMap))
	var blockStoreMap map[string][]string
	// log.Println("upload hashList", hashList)
	_ = client.GetBlockStoreMap(hashList, &blockStoreMap)
	// if err == ERR_SERVER_CRASHED {
	// 	log.Fatalf("Server crashed")
	// }
	// log.Println("upload blockStoreMap", blockStoreMap)
	revBlockStoreMap := reverseBlockStoreMap(blockStoreMap)
	// log.Println("upload revBlockStoreMap", revBlockStoreMap)
	// log.Println("upload revBlockStoreMap length", len(revBlockStoreMap))
	for blockHash, blockData := range blockHashToBlockDataMap {
		blockSize := int32(len(blockData))
		blockObject := Block{BlockData: blockData, BlockSize: blockSize}
		// log.Println("upload blockhash", blockHash)
		blockStoreAddr := revBlockStoreMap[blockHash]
		// log.Println("upload blockStoreAddr", blockStoreAddr)
		var success bool
		err = client.PutBlock(&blockObject, blockStoreAddr, &success)
		if err != nil {
			log.Println("Error while putting block", err)
		}
		if !success {
			log.Println("PutBlock method not successful")
		}
	}
	// Empty file has hashvalue -1
	if numBlocks == 0 {
		hashList = append(hashList, EMPTYFILE_HASHVALUE)
	}
	// log.Println("All Put blocks done")
	var version int32 = 1
	_, localExists := localIndex[fileName]
	if localExists {
		version = 1 + int32(localIndex[fileName].Version)
	}
	var returnedVersion int32
	localFileMetadata := FileMetaData{Filename: fileName, Version: version, BlockHashList: hashList}
	// fmt.Println("to call updateFile")
	err = client.UpdateFile(&localFileMetadata, &returnedVersion)
	// if err == ERR_SERVER_CRASHED {
	// 	log.Fatal("Server crashed")
	// }
	// log.Println("UpdateFile return version", returnedVersion, err)
	if err != nil {
		returnedVersion = -1
	}
	localFileMetadata.Version = returnedVersion
	localIndex[fileName] = &localFileMetadata
	return returnedVersion, err
}

func deleteLocalFile(fileName string, client RPCClient, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData) error {
	localIndex[fileName] = remoteIndex[fileName]
	filePath := filepath.Join(client.BaseDir, fileName)
	if _, err := os.Stat(filePath); err == nil {
		e := os.Remove(filePath)
		if e != nil {
			log.Fatal("Error during removing deleted file", e)
		}
	}
	return nil
}

func deleteFile(fileName string, client RPCClient, localIndex map[string]*FileMetaData, blockStoreAddrs []string) (int32, error) {
	version := localIndex[fileName].Version
	var tombstoneHashList []string = []string{TOMBSTONE_HASHVALUE}
	localFileMetadata := FileMetaData{Filename: fileName, Version: version + 1, BlockHashList: tombstoneHashList}
	var returnedVersion int32
	err := client.UpdateFile(&localFileMetadata, &returnedVersion)
	// if err == ERR_SERVER_CRASHED {
	// 	log.Fatal("Server crashed")
	// }
	// log.Println("UpdateFile return version", returnedVersion, err)
	if err != nil || returnedVersion != version+1 {
		returnedVersion = -1
	} else {
		localFileMetadata.Version = returnedVersion
		localIndex[fileName] = &localFileMetadata
	}
	return returnedVersion, err
}

func isFileDeleted(fileMetaData *FileMetaData) bool {
	if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		return true
	}
	return false
}

func downloadFile(fileName string, client RPCClient, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData, blockStoreAddrs []string) error {
	// Check if the file is deleted in the remote index (TOMBSTONE RECORD)
	if isFileDeleted(remoteIndex[fileName]) {
		// Copy metadata of file
		localIndex[fileName] = remoteIndex[fileName]
		return nil
	}
	localPath := filepath.Join(client.BaseDir, fileName)
	localFile, err := os.Create(localPath)
	if err != nil {
		log.Println("Error while creating file", err)
	}
	defer localFile.Close()
	// Write metadata of file and update localIndex object (not db)
	// err = WriteMetaFile(remoteIndex, client.BaseDir)
	// if err != nil {
	// 	log.Println("Error in updating local index", err)
	// }
	localIndex[fileName] = remoteIndex[fileName]
	// Nothing to write if file is empty
	if len(remoteIndex[fileName].BlockHashList) == 1 && remoteIndex[fileName].BlockHashList[0] == EMPTYFILE_HASHVALUE {
		return nil
	}
	fileContent := ""
	var blockStoreMap map[string][]string
	_ = client.GetBlockStoreMap(remoteIndex[fileName].BlockHashList, &blockStoreMap)
	// if err == ERR_SERVER_CRASHED {
	// 	log.Fatal("Server crashed")
	// }
	revBlockStoreMap := reverseBlockStoreMap(blockStoreMap)
	for _, blockHash := range remoteIndex[fileName].BlockHashList {
		var block Block
		var blockStoreAddr string = revBlockStoreMap[blockHash]
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		// if err == ERR_SERVER_CRASHED {
		// 	log.Fatal("Server crashed")
		// }
		if err != nil {
			log.Println("Error while executing GetBlock call", err)
		}
		fileContent += string(block.BlockData)
	}
	localFile.WriteString(fileContent)
	return nil
}

func reverseBlockStoreMap(blockStoreMap map[string][]string) map[string]string {
	var revBlockStoreMap map[string]string = make(map[string]string)
	for serverAddr, hashes := range blockStoreMap {
		// log.Println("upload serverAddr: ", serverAddr, "len hashes", len(hashes))
		for _, hash := range hashes {
			revBlockStoreMap[hash] = serverAddr
			// log.Println(hash,"->", serverAddr)
		}
	}
	return revBlockStoreMap
}
