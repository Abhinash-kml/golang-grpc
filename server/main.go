package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/grpc-server/users"
	"google.golang.org/grpc"
)

type Server struct {
	users.UnimplementedUserServiceServer
}

func (s *Server) GetUser(ctx context.Context, r *users.UserRequest) (*users.UserResponse, error) {
	return &users.UserResponse{
		Id:   r.Id,
		Name: "Abhinash",
		From: "India",
		Age:  25,
	}, nil
}

func (s *Server) CreateUser(ctx context.Context, r *users.UserCreate) (*users.CreationResult, error) {
	fmt.Print(r)

	return &users.CreationResult{
		Result: "OK",
		Code:   200,
	}, nil
}

func (s *Server) ServerStream(in *users.Empty, stream users.UserService_ServerStreamServer) error {
	duration, _ := time.ParseDuration("1s")
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for range ticker.C {
		err := stream.Send(&users.Message{Payload: fmt.Sprintf("Server Time: %v", time.Now())})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) ClientStream(stream users.UserService_ClientStreamServer) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		fmt.Println("Client time:", message.Payload)
	}
}

func (s *Server) BidirectionalStream(stream users.UserService_BidirectionalStreamServer) error {
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				return
			}

			fmt.Println("BD Stream from client:", message.Payload)
		}
	}()

	go func() {
		duration, _ := time.ParseDuration("1s")
		ticker := time.NewTicker(duration)

		for range ticker.C {
			err := stream.Send(&users.Message{Payload: time.Now().String()})
			if err != nil {
				return
			}
		}
	}()

	return nil
}

func main() {
	fmt.Println("Starting grpc server")

	listener, err := net.Listen("tcp", ":27015")
	if err != nil {
		log.Fatalf("Failed to create lister. Error: %v", err)
	}

	grpcServer := grpc.NewServer()
	users.RegisterUserServiceServer(grpcServer, &Server{})
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to start grpc server. Error: %v", err)
	}
}
