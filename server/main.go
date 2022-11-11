// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	api "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/example/api"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"io"
	"log"
	"net"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const (
	port = ":7777"
)

var tracer = otel.Tracer("grpc-example")

// server is used to implement api.HelloServiceServer.
type server struct {
	api.HelloServiceServer
}

// SayHello implements api.HelloServiceServer.
func (s *server) SayHello(ctx context.Context, in *api.HelloRequest) (*api.HelloResponse, error) {
	log.Printf("Received: %v\n", in.GetGreeting())
	s.workHard(ctx)
	time.Sleep(50 * time.Millisecond)

	return &api.HelloResponse{Reply: "Hello " + in.Greeting}, nil
}

func (s *server) workHard(ctx context.Context) {
	_, span := tracer.Start(ctx, "workHard",
		trace.WithAttributes(attribute.String("extra.key", "extra.value")))
	defer span.End()

	time.Sleep(50 * time.Millisecond)
}

func (s *server) SayHelloServerStream(in *api.HelloRequest, out api.HelloService_SayHelloServerStreamServer) error {
	log.Printf("Received: %v\n", in.GetGreeting())

	for i := 0; i < 5; i++ {
		err := out.Send(&api.HelloResponse{Reply: "Hello " + in.Greeting})
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(i*50) * time.Millisecond)
	}

	return nil
}

func (s *server) SayHelloClientStream(stream api.HelloService_SayHelloClientStreamServer) error {
	i := 0

	for {
		in, err := stream.Recv()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Non EOF error: %v\n", err)
			return err
		}

		log.Printf("Received: %v\n", in.GetGreeting())
		i++
	}

	time.Sleep(50 * time.Millisecond)

	return stream.SendAndClose(&api.HelloResponse{Reply: fmt.Sprintf("Hello (%v times)", i)})
}

func (s *server) SayHelloBidiStream(stream api.HelloService_SayHelloBidiStreamServer) error {
	for {
		in, err := stream.Recv()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Non EOF error: %v\n", err)
			return err
		}

		time.Sleep(50 * time.Millisecond)

		log.Printf("Received: %v\n", in.GetGreeting())
		err = stream.Send(&api.HelloResponse{Reply: "Hello " + in.Greeting})

		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	shutdown := Init()
	defer shutdown()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
	)

	api.RegisterHelloServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Init configures an OpenTelemetry exporter and trace provider.
func Init() func() {
	ctx := context.Background()
	otelAgentAddr, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		otelAgentAddr = "0.0.0.0:4317"
	}
	// If the OpenTelemetry Collector is running on a local cluster (minikube or
	// microk8s), it should be accessible through the NodePort service at the
	// `localhost:30080` endpoint. Otherwise, replace `localhost` with the
	// endpoint of your cluster. If you run the app inside k8s, then you can
	// probably connect directly to the service through dns
	driver := otlpgrpc.NewDriver(
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(otelAgentAddr),
		otlpgrpc.WithDialOption(grpc.WithBlock()), // useful for testing
	)
	exp, err := otlp.NewExporter(ctx, driver)
	handleErr(err, "failed to create exporter")
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String("demo-server-grpc-gateway-v0.20.0"),
		),
	)
	bsp := sdktrace.NewBatchSpanProcessor(exp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tracerProvider)

	return func() {
		// Shutdown will flush any remaining spans and shut down the exporter.
		handleErr(tracerProvider.Shutdown(ctx), "failed to shutdown TracerProvider")
	}
}

func handleErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}
