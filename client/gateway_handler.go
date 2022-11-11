package main

import (
	"context"
	"fmt"
	api "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/example/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log"
	"time"
)

var tracer = otel.Tracer("grpc-example-client")

// server is used to implement api.HelloServiceServer.
type server struct {
	api.HelloServiceServer
	c api.HelloServiceClient
}

func (s *server) workHard(ctx context.Context) {
	_, span := tracer.Start(ctx, "workHard",
		trace.WithAttributes(attribute.String("extra.key", "extra.value")))
	defer span.End()

	time.Sleep(50 * time.Millisecond)
}

// SayHello implements api.HelloServiceServer.
func (s *server) SayHello(ctx context.Context, in *api.HelloRequest) (*api.HelloResponse, error) {
	log.Printf("Received: %v\n", in.GetGreeting())
	res, err := callSayHello(in, s.c)
	if err != nil {
		log.Fatal(err)
	}
	return res, nil
}

func (s *server) SayHelloServerStream(in *api.HelloRequest, out api.HelloService_SayHelloServerStreamServer) error {

	return nil
}

func (s *server) SayHelloClientStream(out api.HelloService_SayHelloClientStreamServer) error {
	return nil
}
func (s *server) SayHelloBidiStream(out api.HelloService_SayHelloBidiStreamServer) error {
	return nil
}

func callSayHello(r *api.HelloRequest, c api.HelloServiceClient) (*api.HelloResponse, error) {
	response, err := c.SayHello(context.Background(), &api.HelloRequest{Greeting: r.Greeting})
	if err != nil {
		return nil, fmt.Errorf("calling SayHello: %w", err)
	}
	log.Printf("Response from server: %s", response.Reply)
	return response, nil
}

func callSayHelloClientStream(c api.HelloServiceClient) error {
	stream, err := c.SayHelloClientStream(context.Background())
	if err != nil {
		return fmt.Errorf("opening SayHelloClientStream: %w", err)
	}

	for i := 0; i < 5; i++ {
		err := stream.Send(&api.HelloRequest{Greeting: "World"})

		time.Sleep(time.Duration(i*50) * time.Millisecond)

		if err != nil {
			return fmt.Errorf("sending to SayHelloClientStream: %w", err)
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("closing SayHelloClientStream: %w", err)
	}

	log.Printf("Response from server: %s", response.Reply)
	return nil
}

func callSayHelloServerStream(c api.HelloServiceClient) error {

	stream, err := c.SayHelloServerStream(context.Background(), &api.HelloRequest{Greeting: "World"})
	if err != nil {
		return fmt.Errorf("opening SayHelloServerStream: %w", err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("receiving from SayHelloServerStream: %w", err)
		}

		log.Printf("Response from server: %s", response.Reply)
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func callSayHelloBidiStream(c api.HelloServiceClient) error {

	stream, err := c.SayHelloBidiStream(context.Background())
	if err != nil {
		return fmt.Errorf("opening SayHelloBidiStream: %w", err)
	}

	serverClosed := make(chan struct{})
	clientClosed := make(chan struct{})

	go func() {
		for i := 0; i < 5; i++ {
			err := stream.Send(&api.HelloRequest{Greeting: "World"})

			if err != nil {
				// nolint: revive  // This acts as its own main func.
				log.Fatalf("Error when sending to SayHelloBidiStream: %s", err)
			}

			time.Sleep(50 * time.Millisecond)
		}

		err := stream.CloseSend()
		if err != nil {
			// nolint: revive  // This acts as its own main func.
			log.Fatalf("Error when closing SayHelloBidiStream: %s", err)
		}

		clientClosed <- struct{}{}
	}()

	go func() {
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				// nolint: revive  // This acts as its own main func.
				log.Fatalf("Error when receiving from SayHelloBidiStream: %s", err)
			}

			log.Printf("Response from server: %s", response.Reply)
			time.Sleep(50 * time.Millisecond)
		}

		serverClosed <- struct{}{}
	}()

	// Wait until client and server both closed the connection.
	<-clientClosed
	<-serverClosed
	return nil
}
