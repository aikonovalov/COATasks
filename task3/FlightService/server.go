package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/artyomkonovalov/task3/gen"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FlightServiceServer struct {
	pb.UnimplementedFlightServiceServer
    db *sql.DB
}

func (s *FlightServiceServer) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if req.GetRoute() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "route is required")
	}

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

	return &pb.SearchFlightsResponse{
		Flights: flights,
	}, nil
}

func (s *FlightServiceServer) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
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

		return &pb.GetFlightResponse{
			Flight: &flight,
		}, nil
	}

	return nil, status.Errorf(codes.NotFound, "flight not found")
}

func (s *FlightServiceServer) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.ReserveSeatsResponse, error) {
    rows, err := s.db.QueryContext(ctx, "SELECT * FROM seat_reservations WHERE flight_id = $1 AND booking_id = $2", req.FlightId, req.BookingId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to reserve seats: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		return nil, status.Errorf(codes.AlreadyExists, "reservation already exists")
	}

	rows2, err := s.db.QueryContext(ctx, "SELECT * FROM flights WHERE id = $1 AND available_seats >= $2", req.FlightId, req.SeatCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to select flight: %v", err)
	}
	defer rows2.Close()

	if !rows2.Next() {
		return nil, status.Errorf(codes.ResourceExhausted, "not enough seats available")
	}

	_, err = s.db.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats - $1 WHERE id = $2", req.SeatCount, req.FlightId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update flight: %v", err)
	}

	_, err = s.db.ExecContext(ctx, "INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status, expires_at) VALUES ($1, $2, $3, 'ACTIVE', $4)", req.FlightId, req.BookingId, req.SeatCount, time.Now().Add(1 * time.Hour))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert reservation: %v", err)
	}

	return &pb.ReserveSeatsResponse{
		Reservation: &pb.SeatReservation{
			ReservationId: req.BookingId,
			FlightId: req.FlightId,
			SeatCount: req.SeatCount,
			Status: pb.SeatReservationStatus_ACTIVE,
			ExpireTs: timestamppb.New(time.Now().Add(1 * time.Hour)),
		},
	}, nil
}

func (s *FlightServiceServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
    rows, err := s.db.QueryContext(ctx, "SELECT flight_id, seat_count FROM seat_reservations WHERE booking_id = $1", req.BookingId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to release reservation: %v", err)
	}
	defer rows.Close()

	var flightId int
	var availableSeats uint32
	
	if !rows.Next() {
		return nil, status.Errorf(codes.NotFound, "reservation not found")
	}

	err = rows.Scan(&flightId, &availableSeats)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to scan reservation: %v", err)
	}

	_, err = s.db.ExecContext(ctx, "UPDATE seat_reservations SET status = $1 WHERE booking_id = $2", pb.SeatReservationStatus_RELEASED, req.BookingId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update reservation: %v", err)
	}

	_, err = s.db.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats + $1 WHERE id = $2", availableSeats, flightId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update flight: %v", err)
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

	lis, err := net.Listen("tcp", ":" + port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()

	flight_service_server := &FlightServiceServer{
		db: db,
	}

	pb.RegisterFlightServiceServer(grpc_server, flight_service_server)

	log.Printf("Listen on port %s", port)
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}