package main

import (
	"context"
	"fmt"
	"log"

	"github.com/grpc-client/users"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(":27015", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create grpc client. Error: %v", err)
	}
	defer conn.Close()

	client := users.NewUserServiceClient(conn)
	request := &users.UserRequest{Id: "1234"}
	response, err := client.GetUser(context.Background(), request)
	if err != nil {
		log.Fatalf("Response error: %v", err)
	}

	fmt.Print(response)
}
