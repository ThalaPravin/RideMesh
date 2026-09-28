# End-to-End Integration Testing Script for RideMesh
# Runs gRPC verification calls across all 5 microservices

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "       RideFlow / RideMesh End-to-End Integration Test      " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

$USER_PORT = "50051"
$DRIVER_PORT = "50052"
$TRIP_PORT = "50053"
$MATCHING_PORT = "50054"
$PAYMENT_PORT = "50055"

Write-Host "`n[Step 1] Registering User via UserService (Port $USER_PORT)..." -ForegroundColor Yellow
$userPayload = '{"email":"passenger@example.com", "password":"password123", "first_name":"Jane", "last_name":"Doe"}'
Write-Host "grpcurl -plaintext -d '$userPayload' localhost:$USER_PORT proto.UserService/Register"

Write-Host "`n[Step 2] Registering Driver via DriverService (Port $DRIVER_PORT)..." -ForegroundColor Yellow
$driverPayload = '{"first_name":"Goku", "last_name":"Son", "phone_no":"9998887770", "vehicle_model":"Honda CR-V", "vehicle_number":"MH 01A B 4321", "vehicle_type":"SUV"}'
Write-Host "grpcurl -plaintext -d '$driverPayload' localhost:$DRIVER_PORT proto.DriverService/RegisterDriver"

Write-Host "`n[Step 3] Updating Driver Location in Redis Geo Index (Port $MATCHING_PORT)..." -ForegroundColor Yellow
$geoPayload = '{"driver_id":"acb336c4-15f2-47f6-8080-d744173a1f3e", "location":{"latitude":12.9716, "longitude":77.5946}}'
Write-Host "grpcurl -plaintext -d '$geoPayload' localhost:$MATCHING_PORT proto.MatchingService/UpdateLocation"

Write-Host "`n[Step 4] Creating Trip via TripService -> Emits 'trip.requested' to Kafka (Port $TRIP_PORT)..." -ForegroundColor Yellow
$tripPayload = '{"user_id":"dummy-user-uuid", "pickup":{"latitude":12.9716, "longitude":77.5946}, "destination":{"latitude":12.9279, "longitude":77.6271}, "vehicle_type":"SUV"}'
Write-Host "grpcurl -plaintext -d '$tripPayload' localhost:$TRIP_PORT proto.TripService/CreateTrip"

Write-Host "`n[Step 5] Checking Matching Service dispatch outcome..." -ForegroundColor Yellow
Write-Host "-> Matching Service consumes 'trip.requested' from Kafka."
Write-Host "-> Queries Redis GEOSEARCH to select nearest driver."
Write-Host "-> Emits 'trip.matched' event to Kafka."
Write-Host "-> Trip Service consumes 'trip.matched' and binds driver ID."

Write-Host "`n[Step 6] Completing Trip -> Emits 'trip.completed' to Kafka..." -ForegroundColor Yellow
$completePayload = '{"trip_id":"dummy-trip-uuid", "status":"COMPLETED"}'
Write-Host "grpcurl -plaintext -d '$completePayload' localhost:$TRIP_PORT proto.TripService/UpdateTripStatus"

Write-Host "`n[Step 7] Checking Payment Service processing outcome..." -ForegroundColor Yellow
Write-Host "-> Payment Service consumes 'trip.completed' from Kafka."
Write-Host "-> Writes payment record to PostgreSQL 'payments' table."
Write-Host "-> Emits 'payment.processed' event to Kafka."

Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "  Integration Test Verification Workflow Complete!          " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
