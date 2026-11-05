package main 

//put uploads a file into the cluster , get downloads it back 
//run client as a container too so that it reaches addresses 
import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const blockSize = 1<<20 //1mb per block

func main(){
	if len(os.Args) != 4 {
		fmt.Println("usage:\n  client put <local-file> <name>\n  client get <name> <local-file>")
		os.Exit(1)
	}
	nn := dialNameNode()
	switch os.Args[1]{
	case "put":
		if err := put(nn, os.Args[2], os.Args[3]); err != nil {
			log.Fatalf("put: %v", err)
		}
	case "get":
		if err := get(nn, os.Args[2], os.Args[3]); err != nil {
			log.Fatalf("get: %v", err)
		}
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}

//now the functions 
func dialNameNode() pb.NameNodeClient {
	addr := os.Getenv("NAMENODE_ADDR")
	if addr == "" {
		addr="localhost:9000"
	}
	conn , err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err !=nil{
		log.Fatalf("dial namenode: %v",err)
	}
	return pb.NewNameNodeClient(conn)
}

func dialDataNode(addr string) (pb.DataNodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewDataNodeClient(conn), conn, nil
}

//put -> split into 1mb blocks , for each ask namenode where , send the firstNode
func put(nn pb.NameNodeClient , localPath , name string) error {
	f , err := os.Open(localPath)
	if err != nil {
		return err 
	}
	defer f.Close()
	buf := make([]byte , blockSize) //open the file and assign the buffer of 1mb 
	count := 0
	
	for{
		n,err:=io.ReadFull(f , buf)
		if err==io.EOF{
			break
		}
		if err != nil && !errors.Is(err , io.ErrUnexpectedEOF){
			return err 
		}
		block := buf[:n]

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		loc, err := nn.AddBlock(ctx, &pb.AddBlockRequest{File: name})
		if err != nil {
			cancel()
			return fmt.Errorf("AddBlock: %w", err)
		}

		dn, conn, err := dialDataNode(loc.Addresses[0])
		if err != nil {
			cancel()
			return err
		}
		_, err = dn.StoreBlock(ctx, &pb.StoreBlockRequest{
			BlockId:  loc.BlockId,
			Data:     block,
			Checksum: crc32.ChecksumIEEE(block),
			Pipeline: loc.Addresses[1:],
		})
		conn.Close()
		cancel()
		if err != nil {
			return fmt.Errorf("StoreBlock %s: %w", loc.BlockId, err)
		}

		count++
		log.Printf("wrote %s (%d bytes) -> %v", loc.BlockId, n, loc.Addresses)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nn.CompleteFile(ctx, &pb.CompleteFileRequest{File: name}); err != nil {
		return fmt.Errorf("CompleteFile: %w", err)
	}
	log.Printf("done: %s in %d blocks", name, count)
	return nil
}

func get(nn pb.NameNodeClient, name, localPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	locs, err := nn.GetBlockLocations(ctx, &pb.GetBlockLocationsRequest{File: name})
	cancel()
	if err != nil {
		return fmt.Errorf("GetBlockLocations: %w", err)
	}

	out, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, b := range locs.Blocks {
		data, err := readBlock(b)
		if err != nil {
			return err
		}
		if _, err := out.Write(data); err != nil {
			return err
		}
		log.Printf("read %s (%d bytes)", b.BlockId, len(data))
	}
	log.Printf("done: %s -> %s", name, localPath)
	return nil
}

// readBlock tries each replica until one returns good data.
func readBlock(b *pb.BlockLocation) ([]byte, error) {
	for _, addr := range b.Addresses {
		dn, conn, err := dialDataNode(addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := dn.ReadBlock(ctx, &pb.ReadBlockRequest{BlockId: b.BlockId})
		cancel()
		conn.Close()

		if err == nil && crc32.ChecksumIEEE(resp.Data) == resp.Checksum {
			return resp.Data, nil
		}
		log.Printf("replica %s failed for %s: %v, trying next", addr, b.BlockId, err)
	}
	return nil, fmt.Errorf("all replicas failed for %s", b.BlockId)
}