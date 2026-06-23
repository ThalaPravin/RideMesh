# PowerShell Script to generate Protobuf gRPC code on Windows

Write-Host "Building Docker-based Protobuf compiler image..." -ForegroundColor Cyan
docker build -f Dockerfile.protobuf -t ridemesh-protoc .

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to build Docker protobuf image."
    exit 1
}

Write-Host "Generating gRPC Go files..." -ForegroundColor Cyan
$pwdPath = (Get-Location).Path

# Run protoc inside the container
docker run --rm -v "${pwdPath}:/workspace" ridemesh-protoc `
    -I. `
    --go_out=. --go_opt=paths=source_relative `
    --go-grpc_out=. --go-grpc_opt=paths=source_relative `
    proto/user.proto proto/driver.proto proto/trip.proto proto/matching.proto proto/payment.proto

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to generate gRPC code."
    exit 1
}

Write-Host "gRPC code generated successfully!" -ForegroundColor Green
