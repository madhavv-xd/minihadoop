package main

import (
	"log"
	"net"
	"time"
	"github.com/madhavv-xd/minihadoop/internal/namenode"
	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc" //pb is grpc related imports
)

func main() {
	lis , err := net.Listen("tcp" , ":9000")
	if err != nil {
		log.Fatalf("listen: %v" , err)
	}

	nn := namenode.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterNameNodeServer(grpcServer , nn)

	go func(){
		for range time.Tick(5 * time.Second) {
			live := nn.LiveNodes(10 * time.Second)
			log.Printf("live nodes: %d", len(live))
			for _ , n := range live{
				log.Printf("  %s %s (last seen %s ago)" , n.ID , n.Address , time.Since(n.LastHeartbeat).Round(time.Second))
			}
		}
	}()

	log.Println("NameNode listening on :9000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}