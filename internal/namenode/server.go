// keeps a table of datanodes (map + rwmutex)
// answers register and heartbeat
// marks nodes dead when they stop heartbeating

package namenode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeInfo struct {
	ID            string
	Address       string
	CapacityBytes int64
	UsedBytes     int64
	FreeSlots     int32
	LastHeartbeat time.Time
	Alive         bool
}

//underreplicated is a block that has fewer live copies than replicaition
type UnderReplicated struct {
	BlockID string 
	LiveNodes []string //live nodes that still hold it  
	Missing int  //how many more copies do we need 
}

type Server struct {
	pb.UnimplementedNameNodeServer

	mu     sync.RWMutex
	nodes  map[string]*NodeInfo
	nextID int

	files     map[string][]string // file -> ordered block IDs
	blocks    map[string][]string // block -> node IDs holding it
	completed map[string]bool     // file -> write finished
	nextBlock int

	wal *editLog
}

func NewServer(walPath string) (*Server,error) {
	s:= &Server{
		nodes:     make(map[string]*NodeInfo),
		files:     make(map[string][]string),
		blocks:    make(map[string][]string),
		completed: make(map[string]bool),
	}
	//new server WAL
	wal , entries , err := openEditLog(walPath)
	if err != nil {
		return nil,err 
	}
	for _,e := range entries{
		s.apply(e)
	}
	s.wal = wal 
	log.Printf("replayed %d edit-log entries: %d nodes, %d files, %d blocks" , len(entries) , len(s.nodes) , len(s.files) , len(s.blocks))
	return s , nil 
}

//apply changes the in-mem maps for one log entry 
//used both in replay and live 
//caller must hold s.mu 
func (s *Server) apply(e logEntry) {
	switch e.Op {
	case "register":
		s.nodes[e.Node] = &NodeInfo{ID: e.Node, Address: e.Addr} // Alive=false until heartbeat
		var n int
		if _, err := fmt.Sscanf(e.Node, "dn-%d", &n); err == nil && n > s.nextID {
			s.nextID = n
		}
	case "add_block":
		s.files[e.File] = append(s.files[e.File], e.Block)
		s.blocks[e.Block] = e.Nodes
		var n int
		if _, err := fmt.Sscanf(e.Block, "blk-%d", &n); err == nil && n > s.nextBlock {
			s.nextBlock = n
		}
	case "complete":
		s.completed[e.File] = true
	case "add_replica":
		s.blocks[e.Block] = append(s.blocks[e.Block], e.Node)
	}
}

func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// same address coming back -> keep its old ID (no log entry needed)
	for _, n := range s.nodes {
		if n.Address == req.Address {
			n.LastHeartbeat = time.Now()
			n.Alive = true
			log.Printf("re-registered %s at %s", n.ID, n.Address)
			return &pb.RegisterResponse{NodeId: n.ID}, nil
		}
	}

	id := fmt.Sprintf("dn-%d", s.nextID+1)
	e := logEntry{Op: "register", Node: id, Addr: req.Address}
	if err := s.wal.append(e); err != nil {
		return nil, status.Errorf(codes.Internal, "edit log: %v", err)
	}
	s.apply(e) // creates the node, bumps nextID

	n := s.nodes[id]
	n.CapacityBytes = req.CapacityBytes
	n.LastHeartbeat = time.Now()
	n.Alive = true

	log.Printf("registered %s at %s", id, req.Address)
	return &pb.RegisterResponse{NodeId: id}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[req.NodeId]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown node, please re-register")
	}
	if !n.Alive {
		log.Printf("DataNode %s is back ALIVE", n.ID)
	}
	n.UsedBytes = req.UsedBytes
	n.FreeSlots = req.FreeSlots
	n.LastHeartbeat = time.Now()
	n.Alive = true

	return &pb.HeartbeatResponse{}, nil
}

// LiveNodes returns a copy of every node currently marked alive.
func (s *Server) LiveNodes(timeout time.Duration) []NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var live []NodeInfo
	for _, n := range s.nodes {
		if n.Alive && time.Since(n.LastHeartbeat) < timeout {
			live = append(live, *n)
		}
	}
	return live
}

// Monitor marks nodes dead if they miss heartbeats for longer than timeout.
// Run it as a goroutine from main: go srv.Monitor(10 * time.Second)
// Monitor marks nodes dead if they miss heartbeats for longer than timeout,
// finds blocks that lost a copy, and re-replicates them.
func (s *Server) Monitor(timeout time.Duration) {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	for range t.C {
		var dead []string
		var under []UnderReplicated

		s.mu.Lock()
		for id, n := range s.nodes {
			if n.Alive && time.Since(n.LastHeartbeat) > timeout {
				n.Alive = false
				dead = append(dead, id)
			}
		}
		if len(dead) > 0 {
			under = s.findUnderReplicatedLocked()
		}
		s.mu.Unlock()

		for _, id := range dead {
			log.Printf("DataNode %s marked DEAD", id)
		}

		for _, u := range under {
			if len(u.LiveNodes) == 0 {
				log.Printf("BLOCK LOST: %s has no live copies", u.BlockID)
				continue
			}
			log.Printf("%s under-replicated: live on %v, need %d more",
				u.BlockID, u.LiveNodes, u.Missing)
			s.replicate(u) // make the missing copies
		}
	}
}

// ---- storage RPCs ----

const replication = 3

func (s *Server) AddBlock(ctx context.Context, req *pb.AddBlockRequest) (*pb.AddBlockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.File == "" {
		return nil, status.Error(codes.InvalidArgument, "file name required")
	}
	if s.completed[req.File] {
		return nil, status.Error(codes.AlreadyExists, "file already written (write-once)")
	}

	var ids, addrs []string
	for id, n := range s.nodes {
		if !n.Alive {
			continue
		}
		ids = append(ids, id)
		addrs = append(addrs, n.Address)
		if len(ids) == replication {
			break
		}
	}
	if len(ids) < replication {
		return nil, status.Errorf(codes.FailedPrecondition,
			"need %d live nodes, have %d", replication, len(ids))
	}

	blockID := fmt.Sprintf("blk-%d", s.nextBlock+1)
	e := logEntry{Op: "add_block", File: req.File, Block: blockID, Nodes: ids}
	if err := s.wal.append(e); err != nil {
		return nil, status.Errorf(codes.Internal, "edit log: %v", err)
	}
	s.apply(e) // adds to files + blocks, bumps nextBlock

	return &pb.AddBlockResponse{BlockId: blockID, Addresses: addrs}, nil
}

func (s *Server) CompleteFile(ctx context.Context, req *pb.CompleteFileRequest) (*pb.CompleteFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.files[req.File]; !ok {
		return nil, status.Error(codes.NotFound, "file not found")
	}
	e := logEntry{Op: "complete", File: req.File}
	if err := s.wal.append(e); err != nil {
		return nil, status.Errorf(codes.Internal, "edit log: %v", err)
	}
	s.apply(e)

	log.Printf("file %s complete (%d blocks)", req.File, len(s.files[req.File]))
	return &pb.CompleteFileResponse{}, nil
}

// GetBlockLocations: for reads, return each block in order with its live node addresses.
func (s *Server) GetBlockLocations(ctx context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.completed[req.File] {
		return nil, status.Error(codes.NotFound, "file not found or not complete")
	}

	resp := &pb.GetBlockLocationsResponse{}
	for _, blockID := range s.files[req.File] {
		loc := &pb.BlockLocation{BlockId: blockID}
		for _, nodeID := range s.blocks[blockID] {
			if n, ok := s.nodes[nodeID]; ok && n.Alive { // skip dead nodes
				loc.Addresses = append(loc.Addresses, n.Address)
			}
		}
		resp.Blocks = append(resp.Blocks, loc)
	}
	return resp, nil
}

//this func scans eveyr block and retrurns the ones with fewer than 
//replication live copes 
//caller must hold s.mu 
func(s *Server) findUnderReplicatedLocked() []UnderReplicated{
	var out []UnderReplicated
	for blockID , holders := range s.blocks {
		var live []string 
		for _ , id := range holders{
			if n,ok := s.nodes[id]; ok && n.Alive {
				live = append(live, id)
			}
		}
		if len(live) < replication {
			out = append(out, UnderReplicated{
				BlockID: blockID,
				LiveNodes: live,
				Missing: replication - len(live),
			})
		}
	}
	return out 
}

// replicate makes the missing copies of one under-replicated block.
func (s *Server) replicate(u UnderReplicated) {
	// --- pick source + targets (under lock) ---
	s.mu.RLock()
	src := s.nodes[u.LiveNodes[0]].Address

	has := make(map[string]bool)
	for _, id := range s.blocks[u.BlockID] { // includes dead holders too
		has[id] = true
	}
	var targetIDs []string
	for id, n := range s.nodes {
		if n.Alive && !has[id] {
			targetIDs = append(targetIDs, id)
			if len(targetIDs) == u.Missing {
				break
			}
		}
	}
	targetAddrs := make([]string, len(targetIDs))
	for i, id := range targetIDs {
		targetAddrs[i] = s.nodes[id].Address
	}
	s.mu.RUnlock()

	if len(targetIDs) == 0 {
		log.Printf("%s: no free node to copy to", u.BlockID)
		return
	}

	// --- talk to the source DataNode (no lock held) ---
	conn, err := grpc.NewClient(src, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%s: dial source %s: %v", u.BlockID, src, err)
		return
	}
	defer conn.Close()
	client := pb.NewDataNodeClient(conn)

	for i, tid := range targetIDs {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := client.ReplicateBlock(ctx, &pb.ReplicateBlockRequest{
			BlockId:       u.BlockID,
			TargetAddress: targetAddrs[i],
		})
		cancel()
		if err != nil {
			log.Printf("%s: copy to %s failed: %v", u.BlockID, tid, err)
			continue
		}

		
		// --- record the new copy (under lock) ---
		s.mu.Lock()
		e := logEntry{Op: "add_replica", Block: u.BlockID, Node: tid}
		if err := s.wal.append(e); err != nil {
			s.mu.Unlock()
			log.Printf("%s: edit log: %v", u.BlockID, err)
			continue
		}
		s.apply(e)
		s.mu.Unlock()
		log.Printf("%s re-replicated to %s", u.BlockID, tid)
	}
}