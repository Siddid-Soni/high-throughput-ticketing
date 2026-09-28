CREATE TYPE seat_status AS ENUM ('AVAILABLE', 'SOLD', 'BLOCKED');

CREATE TYPE booking_state AS ENUM (
    'PENDING',
    'PAYMENT_PENDING',
    'CONFIRMED',
    'EXPIRED',
    'CANCELED',
    'REFUNDED'
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE venues (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    -- e.g. {"sections":[{"name":"A","rows":10,"seats_per_row":20,"type":"standard"}]}
    seat_map_config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE seats (
    id SERIAL PRIMARY KEY,
    venue_id INTEGER NOT NULL REFERENCES venues(id),
    section VARCHAR(50) NOT NULL,
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    seat_number INTEGER NOT NULL CHECK (seat_number > 0),
    type VARCHAR(50) NOT NULL DEFAULT 'standard',
    UNIQUE (venue_id, section, row_number, seat_number)
);

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    venue_id INTEGER NOT NULL REFERENCES venues(id),
    name VARCHAR(255) UNIQUE NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED'
        CHECK (status IN ('SCHEDULED', 'ON_SALE', 'CLOSED', 'CANCELED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX events_venue_id_idx ON events (venue_id);
