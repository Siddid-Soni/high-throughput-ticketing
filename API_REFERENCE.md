# Conference Booking System API Reference

## Overview
A Rust-based conference booking system with waitlist management using RabbitMQ for queue processing and PostgreSQL for data persistence.

## Base URL
```
http://localhost:8080
```

## Data Models

### BookingStatus Enum
```rust
enum BookingStatus {
    CONFIRMED,      // Booking is confirmed and slot is reserved
    WAITLISTED,     // Booking is on waitlist
    CANCELED,       // Booking has been canceled
    ConfirmationPending  // Promoted from waitlist, awaiting confirmation
}
```

### User Model
```rust
struct User {
    user_id: String,        // Primary key, alphanumeric only
}

struct NewUser {
    user_id: String,        // Required, alphanumeric only
    topics: Vec<String>,    // Required, 1-50 topics, alphanumeric + spaces
}
```

### Conference Model
```rust
struct Conference {
    conference_id: i32,
    name: String,                    // Unique, alphanumeric + spaces
    location: String,                // Alphanumeric + spaces
    start_timestamp: NaiveDateTime,  // Format: "YYYY-MM-DD HH:MM:SS"
    end_timestamp: NaiveDateTime,    // Format: "YYYY-MM-DD HH:MM:SS"
    total_slots: i32,               // Total available slots
    available_slots: i32,           // Currently available slots
    created_at: Option<NaiveDateTime>,
}

struct NewConference {
    name: String,           // Required, alphanumeric + spaces
    location: String,       // Required, alphanumeric + spaces
    start: String,          // Required, format: "YYYY-MM-DD HH:MM:SS"
    end: String,            // Required, format: "YYYY-MM-DD HH:MM:SS"
    slots: i32,             // Required, positive integer
    topics: Vec<String>     // Required, 1-10 topics, alphanumeric + spaces
}
```

### Booking Model
```rust
struct Booking {
    booking_id: i32,                                    // Primary key
    conference_id: Option<i32>,                         // Foreign key
    user_id: Option<String>,                            // Foreign key
    status: BookingStatus,                              // Current status
    created_at: Option<NaiveDateTime>,                  // When booking was created
    waitlist_confirmation_deadline: Option<NaiveDateTime>, // Deadline for confirmation
    canceled_at: Option<NaiveDateTime>,                 // When booking was canceled
    can_confirm: Option<bool>,                          // Whether user can confirm
    waitlist_position: Option<i32>,                     // Position in waitlist
}
```

## API Endpoints

### 1. Create User
**POST** `/user`

Creates a new user in the system.

**Request Body:**
```json
{
    "user_id": "string",      // Required, alphanumeric only, unique
    "topics": ["string", ...] // Required, 1-50 items, alphanumeric + spaces
}
```

**Success Response:** `201 Created`
```json
{
    "message": "User added successfully"
}
```

**Error Responses:**
- `400 Bad Request` - Invalid user_id format, missing/invalid topics
- `400 Bad Request` - User already exists

---

### 2. Create Conference
**POST** `/conference`

Creates a new conference with specified slots and schedule.

**Request Body:**
```json
{
    "name": "string",         // Required, alphanumeric + spaces, unique
    "location": "string",     // Required, alphanumeric + spaces
    "start": "YYYY-MM-DD HH:MM:SS", // Required, future timestamp
    "end": "YYYY-MM-DD HH:MM:SS",   // Required, after start time
    "slots": 5,               // Required, positive integer
    "topics": ["string", ...] // Required, 1-10 items, alphanumeric + spaces
}
```

**Success Response:** `201 Created`
```json
{
    "message": "conference added successfully"
}
```

**Error Responses:**
- `400 Bad Request` - Invalid format, missing fields, or conference already exists

---

### 3. Book Conference
**POST** `/book`

Book a conference slot or join the waitlist.

**Request Body:**
```json
{
    "user_id": "string",  // Required, must exist
    "name": "string"      // Required, conference name, must exist
}
```

**Success Response:** `201 Created`
```json
{
    "booking_id": 123,
    "status": "CONFIRMED",        // or "WAITLISTED"
    "message": "Booking confirmed successfully", // or "Added to waitlist"
    "waitlist_position": null     // or integer if waitlisted
}
```

**Error Responses:**
- `400 Bad Request` - User/conference not found, conference started, overlapping booking

---

### 4. Get Booking Status
**GET** `/booking/{booking_id}`

Retrieve detailed status of a specific booking.

**Path Parameters:**
- `booking_id` (integer) - The booking ID

**Success Response:** `200 OK`
```json
{
    "booking_id": 123,
    "status": "ConfirmationPending",
    "conference_name": "Tech Conference 2024",
    "can_confirm": true,
    "confirmation_deadline": "2024-01-15T10:15:30", // or null
    "waitlist_position": null    // or integer if waitlisted
}
```

**Error Responses:**
- `404 Not Found` - Booking not found

---

### 5. Confirm Waitlist Booking
**POST** `/confirm`

Confirm a booking that has been promoted from waitlist (status: ConfirmationPending).

**Security:** Only the user who owns the booking can confirm it.

**Request Body:**
```json
{
    "booking_id": 123,
    "user_id": "user123"  // Must match the booking owner
}
```

**Success Response:** `200 OK`
```json
{
    "message": "Booking confirmed successfully"
}
```

**Error Responses:**
- `400 Bad Request` - Booking not in ConfirmationPending status, conference started, confirmation expired, or **access denied (wrong user)**
- `400 Bad Request` - "Access denied: booking belongs to different user" (security violation)

---

### 6. Cancel Booking
**POST** `/cancel`

Cancel any existing booking (confirmed, waitlisted, or pending confirmation).

**Request Body:**
```json
{
    "booking_id": 123
}
```

**Success Response:** `200 OK`
```json
{
    "message": "Booking canceled successfully"
}
```

**Note:** Canceling a confirmed booking will automatically promote the next person from the waitlist.

---

### 7. Get Conference Bookings
**GET** `/conference/{conference_name}/bookings`

Retrieve all bookings for a specific conference.

**Path Parameters:**
- `conference_name` (string) - The conference name

**Success Response:** `200 OK`
```json
[
    {
        "booking_id": 123,
        "user_id": "user123",
        "status": "CONFIRMED",
        "created_at": "2024-01-15T09:30:00",
        "waitlist_position": null,
        "can_confirm": false,
        "confirmation_deadline": null,
        "canceled_at": null
    },
    {
        "booking_id": 124,
        "user_id": "user456",
        "status": "WAITLISTED",
        "created_at": "2024-01-15T09:35:00",
        "waitlist_position": 1,
        "can_confirm": false,
        "confirmation_deadline": null,
        "canceled_at": null
    }
]
```

**Error Responses:**
- `404 Not Found` - Conference not found

## Waitlist System Behavior

### Automatic Promotion
When a confirmed booking is canceled:
1. System immediately promotes the next person in waitlist (lowest position number)
2. Status changes from `WAITLISTED` to `ConfirmationPending`
3. User has **10 seconds** to confirm using `/confirm` endpoint
4. If not confirmed within 10 seconds, booking moves back to end of waitlist
5. Next person in line is automatically promoted

### Sequential Processing
- Multiple simultaneous cancellations are processed sequentially by RabbitMQ
- Each cancellation promotes exactly one person from waitlist
- No race conditions or double-promotions occur

### Conference Start Cleanup
- When conference start time arrives, all waitlisted bookings are automatically canceled
- Conference-specific queues are cleaned up

## Validation Rules

### User ID
- Must be alphanumeric only (no spaces or special characters)
- Must be unique across system

### Conference Name
- Must be alphanumeric with spaces allowed
- Must be unique across system

### Topics
- Must be alphanumeric with spaces allowed
- Users: 1-50 topics required
- Conferences: 1-10 topics required

### Timestamps
- Must use format: `YYYY-MM-DD HH:MM:SS`
- Conference start must be in the future
- Conference end must be after start

### Business Rules
- Cannot book conference that has already started
- Cannot have overlapping confirmed bookings
- Booking same conference twice is prevented
- Waitlist positions are automatically managed

## Error Response Format
All error responses follow this format:
```json
{
    "message": "Descriptive error message"
}
```

Common HTTP status codes:
- `400 Bad Request` - Invalid input, business rule violation
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error 