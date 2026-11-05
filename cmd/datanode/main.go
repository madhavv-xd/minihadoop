package main

import (
	"log"
	"net"
	"os"

	"github.com/madhavv-xd/minihadoop/internal/datanode"
	pb "github.com/madhavv-xd/minihadoop/proto"
	"google.golang.org/grpc"
)

func main(){
	//read these 3 things from env 
	nameNodeAddr := getenv("NAMENODE_ADDR", "localhost:9000")
	selfAddr := getenv("SELF_ADDR" ,"localhost:9001")
	port := getenv("PORT" , "9001")
	dataDir := getenv("DATA_DIR", "data")
	//BUILD THE DATANODE 
	dn , err := datanode.New(nameNodeAddr , selfAddr ,dataDir)
	if err != nil {
		log.Fatalf("client: %v" , err) 
	}
	//build datanode's own server 
	lis , err := net.Listen("tcp" , ":"+port)
	if err != nil {
		log.Fatalf("listen: %v" , err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDataNodeServer(grpcServer , dn)

	//goroutine func 
	go func(){
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v"  , err) 
		}
	}()

	log.Printf("DataNode on :%s, NameNode at %s" , port , &nameNodeAddr)
	dn.Run()
}

func getenv(key , def string) string{
	if v := os.Getenv(key); v != ""{
		return v
	}
	return def 
}

