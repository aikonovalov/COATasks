package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	api "github.com/artyomkonovalov/task3/BookingService/api"

	pb "github.com/artyomkonovalov/task3/gen"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BookingServer struct {
	db *sql.DB
    flightClient pb.FlightServiceClient
}

func NewBookingServer(db *sql.DB, flightClient pb.FlightServiceClient) *BookingServer {
	return &BookingServer{
		db: db,
		flightClient: flightClient,
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *BookingServer) GetFlights(w http.ResponseWriter, r *http.Request, params api.GetFlightsParams) {
	req := &pb.SearchFlightsRequest{
		Route: &pb.FlightRoute{Origin: params.Origin, Destination: params.Destination},
	}

	if params.Date != nil {
		req.DepartureDate = timestamppb.New(params.Date.Time)
	}

	flights, err := s.flightClient.SearchFlights(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if flights == nil || flights.Flights == nil {
		_ = json.NewEncoder(w).Encode([]any{})
		return
	}
	_ = json.NewEncoder(w).Encode(flights.Flights)
}

func (s *BookingServer) GetFlightsId(w http.ResponseWriter, r *http.Request, id string) {
	resp, err := s.flightClient.GetFlight(r.Context(), &pb.GetFlightRequest{Id: id})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp.Flight)
}

func (s *BookingServer) PostBookings(w http.ResponseWriter, r *http.Request) {
	var req api.CreateBookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flightResp, err := s.flightClient.GetFlight(r.Context(), &pb.GetFlightRequest{Id: req.FlightId})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	flight := flightResp.Flight
	if flight.Status != pb.FlightStatus_SCHEDULED {
		http.Error(w, "Flight is not scheduled", http.StatusBadRequest)
		return
	}

	totalPrice := flight.Price * int64(req.SeatCount)
	passengerEmail := string(req.PassengerEmail)

	var newID int64
	err = s.db.QueryRowContext(r.Context(),
		"INSERT INTO bookings (flight_id, passenger_name, passenger_email, seats, total_price, status) VALUES ($1, $2, $3, $4, $5, 'CONFIRMED') RETURNING id",
		req.FlightId, req.PassengerName, passengerEmail, req.SeatCount, totalPrice,
	).Scan(&newID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = s.flightClient.ReserveSeats(r.Context(), &pb.ReserveSeatsRequest{
		FlightId:  req.FlightId,
		SeatCount: uint32(req.SeatCount),
		BookingId: strconv.FormatInt(newID, 10),
	})
	if err != nil {
		_, _ = s.db.ExecContext(r.Context(), "DELETE FROM bookings WHERE id = $1", newID)
		if status.Code(err) == codes.ResourceExhausted {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "not enough seats available"})
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idStr := strconv.FormatInt(newID, 10)
	status := "CONFIRMED"
	resp := api.Booking{
		Id:             &idStr,
		FlightId:       &req.FlightId,
		PassengerName:  &req.PassengerName,
		PassengerEmail: &passengerEmail,
		SeatCount:      &req.SeatCount,
		TotalPrice:     &totalPrice,
		Status:         &status,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *BookingServer) GetBookings(w http.ResponseWriter, r *http.Request, params api.GetBookingsParams) {
	rows, err := s.db.QueryContext(r.Context(), "SELECT id, flight_id, passenger_name, passenger_email, seats, total_price, status, created_at FROM bookings WHERE passenger_email = $1 ORDER BY id DESC", params.PassengerEmail)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []api.Booking
	for rows.Next() {
		var id int64
		var flightID, passengerName, passengerEmail, status string
		var seats int
		var totalPrice int64
		var createdAt time.Time
		if err := rows.Scan(&id, &flightID, &passengerName, &passengerEmail, &seats, &totalPrice, &status, &createdAt); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		idStr := strconv.FormatInt(id, 10)
		list = append(list, api.Booking{
			Id:             &idStr,
			FlightId:       &flightID,
			PassengerName:  &passengerName,
			PassengerEmail: &passengerEmail,
			SeatCount:      &seats,
			TotalPrice:     &totalPrice,
			Status:         &status,
			CreatedAt:      &createdAt,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}



func (s *BookingServer) GetBookingsId(w http.ResponseWriter, r *http.Request, id string) {
	rows, err := s.db.QueryContext(r.Context(), "SELECT id, flight_id, passenger_name, passenger_email, seats, total_price, status, created_at FROM bookings WHERE id = $1", id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	if !rows.Next() {
		http.Error(w, "Booking not found", http.StatusNotFound)
		return
	}

	var idNum int64
	var flightID, passengerName, passengerEmail, status string
	var seats int
	var totalPrice int64
	var createdAt time.Time

	if err := rows.Scan(&idNum, &flightID, &passengerName, &passengerEmail, &seats, &totalPrice, &status, &createdAt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idStr := strconv.FormatInt(idNum, 10)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(api.Booking{
		Id:             &idStr,
		FlightId:       &flightID,
		PassengerName:  &passengerName,
		PassengerEmail: &passengerEmail,
		SeatCount:      &seats,
		TotalPrice:     &totalPrice,
		Status:         &status,
		CreatedAt:      &createdAt,
	})
}

func (s *BookingServer) PostBookingsIdCancel(w http.ResponseWriter, r *http.Request, id string) {
    _, err := s.db.ExecContext(r.Context(), "UPDATE bookings SET status = $1 WHERE id = $2", "CANCELLED", id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

    _, err = s.flightClient.ReleaseReservation(r.Context(), &pb.ReleaseReservationRequest{BookingId: id})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

    w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode("CANCELLED")
}



func main() {
	log.Println("Starting Booking Service...")
	
	var port string
	flag.StringVar(&port, "port", "8080", "port to listen on")
	flag.Parse()

	log.Println("Connecting to database...")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")

	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		dbHost, dbPort, dbName, dbUser, dbPass)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Println("Connected to database succfly")

	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	flightServiceURL := os.Getenv("FLIGHT_SERVICE_URL")
	if flightServiceURL == "" {
		flightServiceURL = "flight-service:8080"
	}
	
	log.Printf("Connect to Flight Service at %s", flightServiceURL)

	conn, err := grpc.NewClient(flightServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to flight service: %v", err)
	}
	defer conn.Close()
	
	flightClient := pb.NewFlightServiceClient(conn)
	
	server := NewBookingServer(db, flightClient)

	api.HandlerFromMux(server, r)

	log.Printf("Server listening on :%s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
