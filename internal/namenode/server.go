//keeps a table of datanodes(map + rwmutex)
//answers register and heartbeat
//logs which nodes are alive every few seconds

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
)

type NodeInfo struct {
	ID string
	Address string
	CapacityBytes int64
	UsedBytes int64
	FreeSlots int32 
	LastHeartbeat time.Time
}

type Server struct {
	pb.UnimplementedNameNodeServer

	mu sync.RWMutex
	nodes map[string]*NodeInfo
	nextID int 

	files map[string][]string 
	blocks map[string][]string 
	completed map[string]bool 
	nextBlock int 
}

func NewServer() *Server {
	return &Server{nodes: make(map[string]* NodeInfo),
		files: make(map[string][]string),
		blocks: make(map[string][]string),
		completed: make(map[string]bool),
	}
}

func (s *Server) Register (ctx context.Context , req *pb.RegisterRequest) (*pb.RegisterResponse , error){
	s.mu.Lock()
	defer s.mu.Unlock()

	for _,n := range s.nodes {
		if n.Address == req.Address {
			n.LastHeartbeat = time.Now()
			log.Printf("re-registered %s at %s" , n.ID , n.Address)
			return &pb.RegisterResponse{NodeId: n.ID} , nil 
		}
	}
	s.nextID++;
	id := fmt.Sprintf("dn-%d" , s.nextID)
	s.nodes[id] = &NodeInfo{
		ID: id,
		Address: req.Address,
		CapacityBytes: req.CapacityBytes,
		LastHeartbeat: time.Now(),
	}
	log.Printf("registered %s at %s", id, req.Address)
	return &pb.RegisterResponse{NodeId: id}, nil
}

func(s *Server) Heartbeat(ctx context.Context , req *pb.HeartbeatRequest) (*pb.HeartbeatResponse , error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n,ok := s.nodes[req.NodeId]
	if !ok {
		return nil , status.Error(codes.NotFound , "unknown node , please re-register")
	}
	n.UsedBytes = req.UsedBytes
	n.FreeSlots = req.FreeSlots
	n.LastHeartbeat = time.Now() 

	return &pb.HeartbeatResponse{} , nil 
}

func (s *Server) LiveNodes (timeout time.Duration) []NodeInfo{
	s.mu.RLock()
	defer s.mu.RUnlock()

	var live []NodeInfo
	for _,n := range s.nodes {
		if time.Since(n.LastHeartbeat) < timeout {
			live = append(live, *n)
		}
	}
	return live 
}

//now the new 3 rpc's
const replication=3 

func(s*Server) AddBlock(ctx context.Context , req *pb.AddBlockRequest)(*pb.AddBlockResponse , error ){
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.File == "" {
		return nil, status.Error(codes.InvalidArgument, "file name required")
	}

	if s.completed[req.File] {
		return nil, status.Error(codes.AlreadyExists, "file already written (write-once)")
	}
	if len(s.nodes) < replication {
		return nil, status.Errorf(codes.FailedPrecondition,
			"need %d live nodes, have %d", replication, len(s.nodes))
	}
	s.nextBlock++;
	blockId := fmt.Sprintf("blk-%d" , s.nextBlock)

	var ids , addrs []string
	for id ,n := range s.nodes{
		ids = append(ids, id)
		addrs = append(addrs, n.Address)
		if len(ids) == replication {
			break
		}
	}
	s.files[req.File] = append(s.files[req.File], blockId)
	s.blocks[blockId] = ids 

	return &pb.AddBlockResponse{BlockId: blockId , Addresses: addrs} , nil 
}

func (s *Server) CompleteFile(ctx context.Context, req *pb.CompleteFileRequest) (*pb.CompleteFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.files[req.File]; !ok {
		return nil, status.Error(codes.NotFound, "file not found")
	}
	s.completed[req.File] = true
	log.Printf("file %s complete (%d blocks)", req.File, len(s.files[req.File]))
	return &pb.CompleteFileResponse{}, nil
}

// GetBlockLocations: for reads, return each block in order with its node addresses.
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
			if n, ok := s.nodes[nodeID]; ok { // skip nodes that are gone
				loc.Addresses = append(loc.Addresses, n.Address)
			}
		}
		resp.Blocks = append(resp.Blocks, loc)
	}
	return resp, nil
}