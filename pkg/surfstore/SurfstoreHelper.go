package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// Struct containing fileName and Version which uniquely identifies a record in table
type fileNameVersion struct {
	fileName string
	version  int32
}

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

// insert into indexes (fileName, version, hashIndex, hashValue) VALUES ("test.jpg", 1, 1, "aggr");
const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := filepath.Join(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		// log.Println("***", stat)
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error during removing index.db file", e)
		}
	}
	err := createIndexDbFile(outputMetaPath)
	if err != nil {
		log.Fatal("Error while creating index db file", err)
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error during opening index.db file", err)
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During creating prepare statement for createTable", err)
	}
	defer statement.Close()
	_, err = statement.Exec()
	if err != nil {
		log.Fatal("Error while executing the statement", err)
	}
	statement_, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During creating insertTuple prepared statement", err)
	}
	defer statement_.Close()
	for fileName, fileMetaData := range fileMetas {
		version := fileMetaData.Version
		hashList := fileMetaData.BlockHashList
		for hashIndex, hash := range hashList {
			_, err := statement_.Exec(fileName, version, hashIndex, hash)
			if err != nil {
				log.Fatal("Error while executing insert tuple statement", err)
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName, version from indexes;`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue from indexes where fileName="%s" AND version=%d order by hashIndex ASC`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error while querying distinct file names", err)
	}
	var distinctFileNames []fileNameVersion = make([]fileNameVersion, 0)
	for rows.Next() {
		var fileName string
		var version int32
		err := rows.Scan(&fileName, &version)
		if err != nil {
			log.Fatal("Error while scanning the distinct file names rows")
		}
		distinctFileNames = append(distinctFileNames, fileNameVersion{fileName, version})
	}
	rows.Close()
	for _, fileNameVersion := range distinctFileNames {
		rows_, err := db.Query(fmt.Sprintf(getTuplesByFileName, fileNameVersion.fileName, fileNameVersion.version))
		if err != nil {
			log.Fatal("Error while querying tuples of file", err)
		}
		var hashValues []string = make([]string, 0)
		for rows_.Next() {
			var fileName string
			var version int
			var hashIndex int
			var hashValue string
			err := rows_.Scan(&fileName, &version, &hashIndex, &hashValue)
			if err != nil {
				log.Fatal("Error while scanning the ", err)
			}
			hashValues = append(hashValues, hashValue)
		}
		rows_.Close()
		fileMetaMap[fileNameVersion.fileName] = &FileMetaData{Filename: fileNameVersion.fileName, Version: fileNameVersion.version, BlockHashList: hashValues}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
