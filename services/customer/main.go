package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type customerStore struct {
	mu        sync.RWMutex
	byID      map[string]*api.Customer
	emailToID map[string]string
}

func newCustomerStore() *customerStore {
	return &customerStore{
		byID:      make(map[string]*api.Customer),
		emailToID: make(map[string]string),
	}
}

type server struct {
	api.UnimplementedCustomerServiceServer
	store *customerStore
}

func (s *server) CreateCustomer(ctx context.Context, req *api.CreateCustomerRequest) (*api.CreateCustomerReply, error) {
	if req.GetEmail() == "" {
		return nil, fmt.Errorf("email is required")
	}
	if req.GetFirstName() == "" || req.GetLastName() == "" {
		return nil, fmt.Errorf("first_name and last_name are required")
	}

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if existingID, ok := s.store.emailToID[req.GetEmail()]; ok {
		return &api.CreateCustomerReply{Customer: s.store.byID[existingID]}, nil
	}

	id := uuid.NewString()
	c := &api.Customer{
		Id:            id,
		Email:         req.GetEmail(),
		FirstName:     req.GetFirstName(),
		LastName:      req.GetLastName(),
		DefaultAddress: req.GetDefaultAddress(),
		CreatedAtUnix: time.Now().Unix(),
	}

	s.store.byID[id] = c
	s.store.emailToID[req.GetEmail()] = id

	log.Printf("customer created: id=%s email=%s", id, req.GetEmail())
	return &api.CreateCustomerReply{Customer: c}, nil
}

func (s *server) GetCustomer(ctx context.Context, req *api.GetCustomerRequest) (*api.GetCustomerReply, error) {
	if req.GetId() == "" {
		return nil, fmt.Errorf("id is required")
	}

	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	c, ok := s.store.byID[req.GetId()]
	if !ok {
		return nil, fmt.Errorf("customer not found: %s", req.GetId())
	}
	return &api.GetCustomerReply{Customer: c}, nil
}

func main() {
	flagHost := flag.String("host", "127.0.0.1", "address of customer service")
	flagPort := flag.String("port", "50052", "port of customer service")
	flag.Parse()

	addr := fmt.Sprintf("%s:%s", *flagHost, *flagPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	api.RegisterCustomerServiceServer(grpcServer, &server{
		store: newCustomerStore(),
	})

	log.Printf("customer service listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}