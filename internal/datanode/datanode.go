package datanode

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type DataNode struct {
	pb.UnimplementedDataNodeServer

	selfAddr string //address others use to reach this node 
	client pb.NameNodeClient //used to call namenode 
	nodeID string 
	dataDir string //where blocks are stored 
}

func New(nameNodeAddr, selfAddr, dataDir string) (*DataNode, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(nameNodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &DataNode{
		selfAddr: selfAddr,
		client:   pb.NewNameNodeClient(conn),
		dataDir:  dataDir,
	}, nil
} //new node making and linking it to the nameNode client 

func(d *DataNode) register() {
	for{
		ctx , cancel := context.WithTimeout(context.Background() , 2*time.Second)
		//creates a context that is cancelled automatically after 2 seconds , without it 
		//a namenode could hang forever
		resp , err := d.client.Register(ctx , &pb.RegisterRequest{
			Address: d.selfAddr,
			CapacityBytes: 10 << 30,
		})
		cancel() //release the context resources 

		if err == nil {
			d.nodeID = resp.NodeId
			log.Printf("registered as %s" , d.nodeID)
			return 
		}
		log.Printf("register failed: %v(retrying in 2secs)", err)
		time.Sleep(2 * time.Second)
	}
} //registeres the node in respect to the namenode

//running the registered nodes 
func (d *DataNode) Run(){
	d.register() //first register the datanode 
	 
	ticker := time.NewTicker(3 *time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ctx , cancel := context.WithTimeout(context.Background() , 2*time.Second)
		_, err := d.client.Heartbeat(ctx , &pb.HeartbeatRequest{
			NodeId: d.nodeID,
			FreeSlots: 2,
		})
		cancel() //run the cancel in the end 
		if status.Code(err) == codes.NotFound{
		log.Printf("NameNode forgot us , re-registering")
		d.register()
		continue 
	}
	if err != nil {
		log.Printf("heartbeat failed: %v", err) 
		continue
	}
	log.Printf("heartbeat ok (%s)" , d.nodeID)
	}
}

//namenode tells us to copy one of our blocks to another datanode
// ReplicateBlock: NameNode tells us to copy one of our blocks to another DataNode.
func (d *DataNode) ReplicateBlock(ctx context.Context, req *pb.ReplicateBlockRequest) (*pb.ReplicateBlockResponse, error) {
	// read the block from our own disk (checksum verified inside ReadBlock)
	rresp, err := d.ReadBlock(ctx, &pb.ReadBlockRequest{BlockId: req.BlockId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "read %s: %v", req.BlockId, err)
	}

	// connect to the target DataNode
	conn, err := grpc.NewClient(req.TargetAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial %s: %v", req.TargetAddress, err)
	}
	defer conn.Close()

	// send it with the normal StoreBlock RPC (no pipeline: just this one target)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pb.NewDataNodeClient(conn).StoreBlock(cctx, &pb.StoreBlockRequest{
		BlockId:  req.BlockId,
		Data:     rresp.Data,
		Checksum: rresp.Checksum,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store on %s: %v", req.TargetAddress, err)
	}

	log.Printf("replicated %s -> %s", req.BlockId, req.TargetAddress)
	return &pb.ReplicateBlockResponse{Ok: true}, nil
}