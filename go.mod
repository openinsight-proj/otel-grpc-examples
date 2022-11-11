module go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/example

go 1.18

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/otlp v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	google.golang.org/genproto v0.0.0-20211118181313-81c1377c94b1
	google.golang.org/grpc v1.50.1
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/stretchr/testify v1.8.0 // indirect
	go.opentelemetry.io/contrib v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/export/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.20.0 // indirect
	go.opentelemetry.io/proto/otlp v0.7.0 // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20220919091848-fb04ddd9f9c8 // indirect
	golang.org/x/text v0.3.5 // indirect
)

replace go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/example/api => ./api
