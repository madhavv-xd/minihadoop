//a write-ahead edit log: one json line per metadata change , replayed on startup
package namenode

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type logEntry struct {
	Op string `json:"op"`
	Node string `json:"node,omitempty"`
	Addr string `json:"addr,omitempty"`
	File string `json:"file,omitempty"`
	Block string `json:"block,omitempty"`
	Nodes []string `json:"nodes,omitempty"`
}

type editLog struct {
	mu sync.Mutex //many goroutines can write at same time 
	f *os.File
}

//openeditlog reads all existing entries,then opens the file for appending 
func openEditLog(path string) (*editLog , []logEntry , error){
	//returns three things -> the log , the old entries and and error 
	if err := os.MkdirAll(filepath.Dir(path) , 0o755); err != nil{
		return nil , nil , err
	} //filepath gives data , mkdirall creates that folder , if-with-init pattern 

	data,err := os.ReadFile(path) //reads the whole file as bytes into mem
	if err != nil && !os.IsNotExist(err){
		return nil , nil , err 
	}

	var entries []logEntry
	off := 0 //off is the offset , the curr pos in the data 
	for {
		i := bytes.IndexByte(data[off:] , '\n')
		if i<0{
			break
		}
		var e logEntry
		if err := json.Unmarshal(data[off:off+i] , &e); err != nil {
			break
		}
		entries = append(entries, e)
		off += i+1 //append the entries 
	}
	f,err := os.OpenFile(path , os.O_CREATE| os.O_RDWR | os.O_APPEND ,0o644)
	if err != nil {
		return nil,nil,err 
	}
	if err := f.Truncate(int64(off)); err != nil {
		return nil , nil, err 
	}
	return &editLog{f: f}, entries ,nil  
} //returns the edit log 

//append func -> appending each change to it 
func(l *editLog) append(e logEntry) error {
	b,err := json.Marshal(e)
	if err != nil {
		return err 
	}
	b = append(b, '\n') //marshalling happens before the locking and goroutines accessing so that it doesnt slow downt the process 
	l.mu.Lock()
	defer l.mu.Unlock()
	if _,err:= l.f.Write(b); err != nil {
		return err 
	}
	return l.f.Sync()
}
