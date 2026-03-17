CREATE TABLE bookings (
    id SERIAL PRIMARY KEY,

    flight_id VARCHAR(100) NOT NULL,

    passenger_name VARCHAR(100) NOT NULL,
    passenger_email VARCHAR(100) NOT NULL,

    seats INT NOT NULL CHECK (seats > 0),
    total_price BIGINT NOT NULL CHECK (total_price > 0),

    status VARCHAR(20) NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bookings_flight ON bookings(flight_id);
CREATE INDEX idx_bookings_status ON bookings(status);
CREATE INDEX idx_bookings_passenger_email ON bookings(passenger_email);
