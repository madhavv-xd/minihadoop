package datanode

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

//imports and path helpers 
func(d *DataNode) blockPath(id string) string {
	return filepath.Join(d.dataDir , id)
}

func(d *DataNode) sumPath(id string) string {
	return filepath.Join(d.dataDir , id+".crc")
} //checksum 

//store block->verify,save locally , then foreard to next node in the pipeline
func (d*DataNode) StoreBlock(ctx context.Context , req *pb.StoreBlockRequest) (*pb.StoreBlockResponse , error){
	if req.BlockId == ""{
		return nil , status.Error(codes.InvalidArgument , "block_id required")
	}
	//data must match the checksum the client sent 
	if crc32.ChecksumIEEE(req.Data) != req.Checksum {
		return nil , status.Error(codes.DataLoss , "checksum mismatch or recerive")
	}
	if err := d.saveBlock(req.BlockId, req.Data, req.Checksum); err != nil {
		return nil, status.Errorf(codes.Internal, "save: %v", err)
	}
	log.Printf("stored %s (%d bytes)", req.BlockId, len(req.Data))
	if len(req.Pipeline) > 0 {
		if err := forward(ctx, req.Pipeline[0], &pb.StoreBlockRequest{
			BlockId:  req.BlockId,
			Data:     req.Data,
			Checksum: req.Checksum,
			Pipeline: req.Pipeline[1:],
		}); err != nil {
			return nil, status.Errorf(codes.Unavailable, "forward to %s: %v", req.Pipeline[0], err)
		}
	}
	return &pb.StoreBlockResponse{Ok: true}, nil
}

//fowarding 
func forward(ctx context.Context , addr string , req *pb.StoreBlockRequest) error {
	conn , err := grpc.NewClient(addr , grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err 
	}
	defer conn.Close()

	ctx,cancel := context.WithTimeout(ctx , 10*time.Second)
	defer cancel()
	_,err = pb.NewDataNodeClient(conn).StoreBlock(ctx,req)
	return err 
}

//saveBlock->writes to a temp file first , then rename , so a crash never leaves a half-written one
func(d *DataNode) saveBlock(id string , data []byte , sum uint32) error {
	tmp := d.blockPath(id) + ".tmp"
	if err := os.WriteFile(tmp,data, 0o644); err != nil {
		return err 
	}
	if err := os.Rename(tmp, d.blockPath(id)); err != nil {
		return err 
	}
	buf := make([]byte , 4)
	binary.BigEndian.PutUint32(buf , sum)
	return os.WriteFile(d.sumPath(id),  buf , 0o644)
}	

func (d *DataNode) ReadBlock(ctx context.Context, req *pb.ReadBlockRequest) (*pb.ReadBlockResponse, error) {
	data, err := os.ReadFile(d.blockPath(req.BlockId))
	if os.IsNotExist(err) {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read: %v", err)
	}

	buf, err := os.ReadFile(d.sumPath(req.BlockId))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read checksum: %v", err)
	}
	sum := binary.BigEndian.Uint32(buf)

	if crc32.ChecksumIEEE(data) != sum {
		log.Printf("CORRUPT block %s", req.BlockId)
		return nil, status.Error(codes.DataLoss, "block corrupt on disk")
	}
	return &pb.ReadBlockResponse{Data: data, Checksum: sum}, nil
}