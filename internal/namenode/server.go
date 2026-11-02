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
}

func NewServer() *Server {
	return &Server{nodes: make(map[string]* NodeInfo)}
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