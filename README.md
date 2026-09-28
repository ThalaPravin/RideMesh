# RideFlow: Distributed Ride-Hailing Platform

RideFlow is a high-performance, event-driven, distributed ride-hailing backend platform engineered in Go. Designed with scalability and reliability in mind, it decomposes the typical ride-hailing lifecycle into five distinct, loosely-coupled microservices.

## 🚀 Key Features
* **Distributed Microservices**: User, Driver, Trip, Matching, and Payment services written in Go, structured and managed via **Uber Fx** dependency injection.
* **Low-Latency Geospatial Queries**: Real-time driver location tracking, state management, and proximity searches using **Redis Geospatial indexes** (`GEOADD`, `GEOSEARCH`).
* **Event-Driven Workflows**: Asynchronous decoupling of ride requests, driver dispatch, status updates, and payment processing using **Apache Kafka** event streams.
* **Synchronous Inter-service Communication**: Fast, schema-enforced, and strongly typed **gRPC** interfaces.
* **Production-Grade Infrastructure**: Clean database normalization in **PostgreSQL**, containerized services, and cloud-ready **Kubernetes** configurations.

---

## 🏗️ System Architecture

RideFlow is structured to optimize transactional throughput and maintain low-latency response times for matching. Below is the layout of the communication paths and event flows between the services:

```mermaid
graph TD
    UserClient[User App / Client] -->|gRPC| UserService[User Service]
    UserClient -->|gRPC| TripService[Trip Service]
    DriverClient[Driver App / Client] -->|gRPC| DriverService[Driver Service]
    DriverClient -->|gRPC| MatchingService[Matching Service - Geo Updates]

    %% Databases
    UserService -->|SQL| UserDB[(PostgreSQL - Users)]
    DriverService -->|SQL| DriverDB[(PostgreSQL - Drivers)]
    TripService -->|SQL| TripDB[(PostgreSQL - Trips)]
    PaymentService -->|SQL| PaymentDB[(PostgreSQL - Payments)]
    
    %% Cache
    MatchingService -->|Redis Geo| RedisCache[(Redis - Driver Locations)]
    
    %% Apache Kafka Events
    TripService -->|Publish: trip.requested| Kafka[Kafka Event Broker]
    Kafka -->|Subscribe: trip.requested| MatchingService
    MatchingService -->|Publish: trip.matched| Kafka
    Kafka -->|Subscribe: trip.matched| TripService
    TripService -->|Publish: trip.completed| Kafka
    Kafka -->|Subscribe: trip.completed| PaymentService
    PaymentService -->|Publish: payment.processed| Kafka
    Kafka -->|Subscribe: payment.processed| TripService
```

---

## ⚡ Apache Kafka Event-Driven Architecture

Kafka acts as the central event bus enabling eventual consistency across microservices without blocking HTTP/gRPC callers.

### Event Topics & Message Flow

| Topic Name | Publisher Service | Subscriber Service | Trigger Condition | Business Action Taken |
| :--- | :--- | :--- | :--- | :--- |
| `trip.requested` | **Trip Service** | **Matching Service** | User submits a ride request | Triggers Redis geospatial search for nearest available driver |
| `trip.matched` | **Matching Service** | **Trip Service** | Eligible driver located nearby | Updates trip state to `ASSIGNED` and binds driver_id |
| `trip.completed` | **Trip Service** | **Payment Service** | Driver completes the trip | Triggers automatic payment processing pipeline |
| `payment.processed` | **Payment Service** | **Trip Service** | Payment gateway responds (Success/Fail) | Updates trip payment status and closes trip lifecycle |

### Event Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    actor Passenger
    participant TripService as Trip Service
    participant Kafka as Kafka Broker
    participant MatchingService as Matching Service
    participant Redis as Redis Cache
    participant PaymentService as Payment Service

    Passenger->>TripService: CreateTrip(pickup, destination)
    TripService->>TripService: Save Trip (Status: REQUESTED)
    TripService->>Kafka: Publish "trip.requested" event
    TripService-->>Passenger: Return Trip Response (Status: REQUESTED)

    Kafka->>MatchingService: Consume "trip.requested"
    MatchingService->>Redis: GEOSEARCH pickup location
    Redis-->>MatchingService: Return nearest driver_id
    MatchingService->>Kafka: Publish "trip.matched" (trip_id, driver_id)

    Kafka->>TripService: Consume "trip.matched"
    TripService->>TripService: Update Trip (Status: ASSIGNED, driver_id)

    note over Passenger,TripService: Trip in progress...

    Passenger->>TripService: UpdateTripStatus(COMPLETED)
    TripService->>TripService: Save Trip (Status: COMPLETED)
    TripService->>Kafka: Publish "trip.completed" event

    Kafka->>PaymentService: Consume "trip.completed"
    PaymentService->>PaymentService: Process Payment
    PaymentService->>Kafka: Publish "payment.processed" (status: SUCCESS)

    Kafka->>TripService: Consume "payment.processed"
    TripService->>TripService: Update Trip (Payment: PAID)
```

### Event Payload Schema Sample (`trip.requested`)

```json
{
  "event_id": "evt_9a8b7c6d-5e4f-3a2b-1c0d",
  "event_type": "TRIP_REQUESTED",
  "timestamp": "2026-06-24T00:30:00Z",
  "payload": {
    "trip_id": "trip_12345678-abcd-ef01-2345",
    "user_id": "user_87654321-fedc-ba98-7654",
    "pickup": { "latitude": 12.9716, "longitude": 77.5946 },
    "destination": { "latitude": 12.9279, "longitude": 77.6271 },
    "vehicle_type": "SUV",
    "amount": 350.00
  }
}
```

---

## 📦 Microservices Breakdown

### 1. User Service
* **Responsibility**: Manages rider accounts, security, and profile metadata.
* **Tech Stack**: Go, Uber Fx, gRPC, PostgreSQL, JWT, bcrypt.
* **Key Feature**: Schema-enforced user profiles, bcrypt password hashing, and signed JWT authentication.

### 2. Driver Service
* **Responsibility**: Manages driver profiles, vehicle associations (cabs), rating statistics, and active shifts.
* **Tech Stack**: Go, Uber Fx, gRPC, PostgreSQL.
* **Key Feature**: Tracks vehicle configurations (Micro, Mini, SUV, Luxury), shift status (`is_online`), and driver ratings.

### 3. Trip Service
* **Responsibility**: Acts as the central coordinator of the ride lifecycle.
* **Tech Stack**: Go, Uber Fx, gRPC, PostgreSQL, Kafka Producer/Consumer.
* **Key Feature**: Orchestrates states (`REQUESTED`, `ASSIGNED`, `ARRIVED`, `STARTED`, `COMPLETED`, `CANCELED`) and publishes lifecycle events to Kafka.

### 4. Matching Service
* **Responsibility**: Pairs passenger ride requests with nearby, online, and eligible drivers.
* **Tech Stack**: Go, Uber Fx, gRPC, Redis (Geospatial), Kafka Consumer/Producer.
* **Key Feature**: Consumes `trip.requested` events, queries Redis using `GEOSEARCH` to find drivers within a given radius, assigns the closest driver, and publishes `trip.matched`.

### 5. Payment Service
* **Responsibility**: Performs transaction processing once a ride is completed.
* **Tech Stack**: Go, Uber Fx, gRPC, PostgreSQL, Kafka Consumer/Producer.
* **Key Feature**: Consumes `trip.completed` events, processes billing transactions, records receipts in Postgres, and publishes `payment.processed`.

---

## 🛠️ Technology Stack & Rationale

* **Go (Golang)**: Chosen for its lightweight footprint, fast compilation, and superior concurrency model (goroutines) suitable for network-heavy microservices.
* **Uber Fx**: A modular dependency injection framework that standardizes service bootstrap, dependency wiring, and graceful shutdown lifecycles.
* **gRPC / Protocol Buffers**: Provides high-performance, binary-serialized synchronous communication, avoiding the overhead of JSON parsing over HTTP/1.1.
* **Apache Kafka**: Serves as the backbone for the platform's eventual consistency. Enables high-throughput message ingestion and fault-tolerant event processing.
* **Redis**: Used specifically for its sub-millisecond read/write speeds on geospatial coordinates, critical for tracking high-frequency driver ping locations.
* **PostgreSQL**: Used for transactional consistency (ACID compliant) to store customer accounts, driver stats, trip records, and payment receipts.

---

## 🗺️ Implementation Roadmap

* **Phase 1**: Workspace Monorepo Setup, Protobuf definitions, Docker-Compose, Uber Fx stubs. *(Completed)*
* **Phase 2**: User & Driver services implementation + PostgreSQL Schema & Repositories + JWT Auth. *(Completed)*
* **Phase 3**: Trip lifecycle management & Kafka event broker wiring. *(In Progress)*
* **Phase 4**: Redis Geospatial tracking & Matching engine.
* **Phase 5**: Payment processing, mock gateway, and completion hooks.
* **Phase 6**: Containerization (Docker), Kubernetes manifests, Prometheus logging, and observability dashboards.
