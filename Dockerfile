# Multi-stage Dockerfile for RideMesh Microservices

FROM golang:1.21-alpine AS builder

# Install protobuf compiler and build dependencies
RUN apk add --no-cache protobuf git

# Install protobuf plugins for Go
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

WORKDIR /app

# Copy Go dependency files
COPY go.mod ./
# Note: go.sum will be generated on build since we didn't use go tidy yet. We will run go mod tidy inside the container.
RUN go mod tidy

# Copy all source files
COPY . .

# Generate gRPC Go code inside the build container
RUN protoc -I. \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/user.proto proto/driver.proto proto/trip.proto proto/matching.proto proto/payment.proto

# ARG to specify which service to build
ARG SERVICE_NAME

# Compile the specified service binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/ridemesh-service ./services/${SERVICE_NAME}

# Final lightweight runner image
FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy the compiled binary
COPY --from=builder /app/ridemesh-service .

# Run the binary
CMD ["./ridemesh-service"]
