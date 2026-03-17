package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/artyomkonovalov/task3/gen"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FlightServiceServer struct {
	pb.UnimplementedFlightServiceServer
    db    *sql.DB
	redis *redis.Client
}

func (s *FlightServiceServer) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if req.GetRoute() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "route is required")
	}

	var cacheKey string
	if s.redis != nil {
		datePart := "any"
		if req.GetDepartureDate() != nil {
			dd := req.DepartureDate.AsTime()
			datePart = dd.Format("2006-01-02")
		}
		cacheKey = fmt.Sprintf("search:%s:%s:%s", req.Route.Origin, req.Route.Destination, datePart)
		if data, err := s.redis.Get(ctx, cacheKey).Bytes(); err == nil {
			var cached []*pb.Flight
			if err := json.Unmarshal(data, &cached); err == nil {
				log.Printf("SearchFlights via redis")
				return &pb.SearchFlightsResponse{Flights: cached}, nil
			}
		}
	}

	log.Printf("SearchFlights via db")

	var rows *sql.Rows
	var err error
	if req.GetDepartureDate() != nil {
		dd := req.DepartureDate.AsTime()
		departureDate := time.Date(dd.Year(), dd.Month(), dd.Day(), 0, 0, 0, 0, time.UTC)
		rows, err = s.db.QueryContext(
			ctx,
			"SELECT id, airline, flight_number, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status FROM flights WHERE origin = $1 AND destination = $2 AND departure_date = $3",
			req.Route.Origin,
			req.Route.Destination,
			departureDate,
		)
	} else {
		rows, err = s.db.QueryContext(
			ctx,
			"SELECT id, airline, flight_number, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status FROM flights WHERE origin = $1 AND destination = $2",
			req.Route.Origin,
			req.Route.Destination,
		)
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to search flights: %v", err)
	}
	defer rows.Close()

	flights := make([]*pb.Flight, 0)
	for rows.Next() {
		var flight pb.Flight
		var origin string
		var destination string
		var flightStatus string
		var departureTime time.Time
		var arrivalTime time.Time

		err := rows.Scan(&flight.Id, &flight.Airline, &flight.FlightNumber, &origin, &destination, &departureTime, &arrivalTime, &flight.TotalSeats, &flight.AvailableSeats, &flight.Price, &flightStatus)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan flight: %v", err)
		}

		flight.Route = &pb.FlightRoute{
			Origin: origin,
			Destination: destination,
		}

		flight.DepartureTime = timestamppb.New(departureTime)
		flight.ArrivalTime = timestamppb.New(arrivalTime)
		if v, ok := pb.FlightStatus_value[flightStatus]; ok {
			flight.Status = pb.FlightStatus(v)
		} else {
			flight.Status = pb.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
		}

		flights = append(flights, &flight)
	}

	if s.redis != nil && cacheKey != "" {
		if data, err := json.Marshal(flights); err == nil {
			_ = s.redis.Set(ctx, cacheKey, data, 5*time.Minute).Err()
		}
	}

	return &pb.SearchFlightsResponse{Flights: flights}, nil
}

func (s *FlightServiceServer) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
	cacheKey := ""
	if s.redis != nil {
		cacheKey = fmt.Sprintf("flight:%s", req.Id)
		if data, err := s.redis.Get(ctx, cacheKey).Bytes(); err == nil {
			var f pb.Flight
			if err := json.Unmarshal(data, &f); err == nil {
				log.Printf("GetFlight via redis")
				return &pb.GetFlightResponse{Flight: &f}, nil
			}
		}
	}

	log.Printf("GetFlight via db")

    rows, err := s.db.QueryContext(ctx, "SELECT id, airline, flight_number, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status FROM flights WHERE id = $1", req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "GOOOOOL %v", err)
	}
	defer rows.Close()
	
	if rows.Next() {
		var flight pb.Flight
		var origin string
		var destination string
		var flightStatus string
		var departureTime time.Time
		var arrivalTime time.Time

		err := rows.Scan(&flight.Id, &flight.Airline, &flight.FlightNumber, &origin, &destination, &departureTime, &arrivalTime, &flight.TotalSeats, &flight.AvailableSeats, &flight.Price, &flightStatus)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan flight: %v", err)
		}

		flight.Route = &pb.FlightRoute{
			Origin: origin,
			Destination: destination,
		}

		flight.DepartureTime = timestamppb.New(departureTime)
		flight.ArrivalTime = timestamppb.New(arrivalTime)
		if v, ok := pb.FlightStatus_value[flightStatus]; ok {
			flight.Status = pb.FlightStatus(v)
		} else {
			flight.Status = pb.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
		}

		if s.redis != nil && cacheKey != "" {
			if data, err := json.Marshal(&flight); err == nil {
				_ = s.redis.Set(ctx, cacheKey, data, 5*time.Minute).Err()
			}
		}

		return &pb.GetFlightResponse{Flight: &flight}, nil
	}

	return nil, status.Errorf(codes.NotFound, "flight not found")
}

func (s *FlightServiceServer) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.ReserveSeatsResponse, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start tx: %v", err)
	}
	defer tx.Rollback()

	var is_exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM seat_reservations WHERE booking_id = $1 AND status = 'ACTIVE')", req.BookingId).Scan(&is_exists); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check existing reservation: %v", err)
	}
	if is_exists {
		return nil, status.Errorf(codes.AlreadyExists, "reservation already exists")
	}

	var available int
	err = tx.QueryRowContext(ctx, "SELECT available_seats FROM flights WHERE id = $1 FOR UPDATE", req.FlightId).Scan(&available)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "flight not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to lock flight: %v", err)
	}

	if available < int(req.SeatCount) {
		return nil, status.Errorf(codes.ResourceExhausted, "not enough seats available")
	}

	_, err = tx.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats - $1 WHERE id = $2", req.SeatCount, req.FlightId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update flight: %v", err)
	}

	expiresAt := time.Now().Add(1 * time.Hour)
	_, err = tx.ExecContext(ctx, "INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status, expires_at) VALUES ($1, $2, $3, 'ACTIVE', $4)", req.FlightId, req.BookingId, req.SeatCount, expiresAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert reservation: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit tx: %v", err)
	}

	if s.redis != nil {
		_ = s.redis.Del(ctx, fmt.Sprintf("flight:%s", req.FlightId)).Err()
		_ = s.redis.FlushDB(ctx).Err()
	}

	return &pb.ReserveSeatsResponse{
		Reservation: &pb.SeatReservation{
			ReservationId: req.BookingId,
			FlightId: req.FlightId,
			SeatCount: req.SeatCount,
			Status: pb.SeatReservationStatus_ACTIVE,
			ExpireTs: timestamppb.New(expiresAt),
		},
	}, nil
}

func (s *FlightServiceServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start tx: %v", err)
	}
	defer tx.Rollback()

	var flightID string
	var seatCount int
	err = tx.QueryRowContext(ctx, "SELECT flight_id, seat_count FROM seat_reservations WHERE booking_id = $1 AND status = 'ACTIVE' FOR UPDATE", req.BookingId).Scan(&flightID, &seatCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "reservation not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to select reservation: %v", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE seat_reservations SET status = 'RELEASED' WHERE booking_id = $1", req.BookingId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update reservation: %v", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats + $1 WHERE id = $2", seatCount, flightID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update flight: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit tx: %v", err)
	}

	if s.redis != nil {
		_ = s.redis.Del(ctx, fmt.Sprintf("flight:%s", flightID)).Err()
		_ = s.redis.FlushDB(ctx).Err()
	}

	return &pb.ReleaseReservationResponse{
		Status: pb.SeatReservationStatus_RELEASED,
	}, nil
}

func main() {
	log.Println("Start FlightService")
	
	var port string
	flag.StringVar(&port, "port", "8080", "port to listen on")
	flag.Parse()

	log.Printf("Connect to database")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", dbHost, dbPort, dbName, dbUser, dbPass))
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Println("Connected to database succfly")

	var rdb *redis.Client
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Printf("Redis disabled: %v", err)
			rdb = nil
		} else {
			log.Printf("Connected to Redis at %s", redisAddr)
		}
	}

	lis, err := net.Listen("tcp", ":" + port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer(
		grpc.UnaryInterceptor(authMiddleware),
	)

	flight_service_server := &FlightServiceServer{
		db:    db,
		redis: rdb,
	}

	pb.RegisterFlightServiceServer(grpc_server, flight_service_server)

	log.Printf("Listen on port %s", port)
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func authMiddleware(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	expected := os.Getenv("FLIGHT_API_KEY")
	if expected == "" {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md["flight-api-key"]
	if len(values) == 0 || values[0] != expected {
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	}

	return handler(ctx, req)
}