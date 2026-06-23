.PHONY: build-proto-image gen-proto build-services up down clean

# Image tag for protobuf compilation
PROTO_IMAGE=ridemesh-protoc

build-proto-image:
	docker build -f Dockerfile.protobuf -t $(PROTO_IMAGE) .

gen-proto:
	docker run --rm -v "$(shell pwd):/workspace" $(PROTO_IMAGE) \
		-I. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/user.proto proto/driver.proto proto/trip.proto proto/matching.proto proto/payment.proto

build-services:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	rm -rf proto/*.pb.go
