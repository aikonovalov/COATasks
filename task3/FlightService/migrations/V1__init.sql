CREATE TABLE flights (
    id SERIAL PRIMARY KEY,

    flight_number VARCHAR(10) NOT NULL,
    airline VARCHAR(100) NOT NULL,

    origin VARCHAR(3) NOT NULL,
    destination VARCHAR(3) NOT NULL,

    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP NOT NULL,

    departure_date DATE NOT NULL,
    
    UNIQUE(flight_number, departure_date),

    total_seats INT NOT NULL CHECK (total_seats > 0),
    available_seats INT NOT NULL CHECK (available_seats >= 0),

    price BIGINT NOT NULL CHECK (price > 0),

    status VARCHAR(20) NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_flights_route ON flights(origin, destination, departure_date);
CREATE INDEX idx_flights_status ON flights(status);

CREATE TABLE seat_reservations (
    id SERIAL PRIMARY KEY,

    flight_id VARCHAR(100) NOT NULL,
    booking_id VARCHAR(100) NOT NULL,

    seat_count INT NOT NULL CHECK (seat_count > 0),

    status VARCHAR(20) NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_reservations_booking ON seat_reservations(booking_id);
CREATE INDEX idx_reservations_flight ON seat_reservations(flight_id);
CREATE INDEX idx_reservations_status ON seat_reservations(status);
