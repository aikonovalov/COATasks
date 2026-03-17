INSERT INTO flights (
    flight_number,
    airline,
    origin,
    destination,
    departure_time,
    arrival_time,
    departure_date,
    total_seats,
    available_seats,
    price,
    status
)

VALUES
    ('SU100', 'Aeroflot', 'LED', 'MSQ', '2026-03-17 10:00:00', '2026-03-17 11:30:00', '2026-03-17', 180, 180, 12000, 'SCHEDULED'),
    ('SU101', 'Aeroflot', 'LED', 'MSQ', '2026-03-17 18:00:00', '2026-03-17 19:30:00', '2026-03-17', 180,  50, 11000, 'SCHEDULED'),
    ('B2100', 'Belavia',  'MSQ', 'LED', '2026-03-18 09:00:00', '2026-03-18 10:30:00', '2026-03-18', 120, 120, 9000,  'SCHEDULED');

