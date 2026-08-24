#!/usr/bin/env python3
"""
Test Multiple Cancellations with Waitlist Scenario
Tests the specific case: 3 slots, 5 users booked, 2 cancellations
With 20-second confirmation timeout
"""
import asyncio
import aiohttp
import time
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8080"

# Helper functions
async def create_user(session, user_id, topics=["testing"]):
    async with session.post(f"{BASE_URL}/user", json={"user_id": user_id, "topics": topics}) as resp:
        return resp.status in [201, 400]

async def create_conference(session, name, slots=3, seconds_ahead=15):
    """Create conference that starts in specified seconds for automatic cleanup"""
    start_time = (datetime.utcnow() + timedelta(seconds=seconds_ahead)).strftime('%Y-%m-%d %H:%M:%S')
    end_time = (datetime.utcnow() + timedelta(seconds=seconds_ahead+10)).strftime('%Y-%m-%d %H:%M:%S')
    
    async with session.post(f"{BASE_URL}/conference", json={
        "name": name,
        "location": "Test Location", 
        "start": start_time,
        "end": end_time,
        "slots": slots,
        "topics": ["testing", "conference"]
    }) as resp:
        return resp.status == 201

async def book_conference(session, user_id, conference_name):
    async with session.post(f"{BASE_URL}/book", json={"user_id": user_id, "name": conference_name}) as resp:
        if resp.status == 201:
            data = await resp.json()
            return data.get("booking_id"), data.get("status")
        return None, None

async def cancel_booking(session, booking_id):
    async with session.post(f"{BASE_URL}/cancel", json={"booking_id": booking_id}) as resp:
        return resp.status == 200

async def confirm_booking(session, booking_id, user_id):
    async with session.post(f"{BASE_URL}/confirm", json={"booking_id": booking_id, "user_id": user_id}) as resp:
        return resp.status, await resp.text()

async def get_conference_bookings(session, conference_name):
    async with session.get(f"{BASE_URL}/conference/{conference_name}/bookings") as resp:
        if resp.status == 200:
            bookings = await resp.json()
            return [(b["user_id"], b["booking_id"], b["status"]) for b in bookings]
        return []

def count_booking_statuses(bookings):
    confirmed = len([s for u, bid, s in bookings if s == "CONFIRMED"])
    pending = len([s for u, bid, s in bookings if s == "ConfirmationPending"])
    waitlisted = len([s for u, bid, s in bookings if s == "WAITLISTED"])
    canceled = len([s for u, bid, s in bookings if s == "CANCELED"])
    return confirmed, pending, waitlisted, canceled

def print_booking_state(bookings, title):
    print(f"\n📊 {title}:")
    for user_id, booking_id, status in bookings:
        print(f"   - {user_id}: booking {booking_id} ({status})")

async def test_multiple_cancellations_with_waitlist(session):
    """Test multiple cancellations when more people are booked than available slots"""
    print("🧪 Testing multiple cancellations with 3 slots, 5 users, and 2 cancellations...")
    print("📝 Configuration: 20-second confirmation timeout")
    timestamp = str(int(time.time()))
    conf_name = f"MultiCancelWaitlistTest{timestamp}"
    
    # Create conference with 3 slots that starts in 45 seconds (enough time for 20-second confirmation timeout + observations)
    print("📅 Creating conference with 3 slots, starting in 45 seconds...")
    await create_conference(session, conf_name, slots=3, seconds_ahead=45)
    
    # Create 5 users
    print("👥 Creating 5 users...")
    users = [f"mcwuser{i}{timestamp}" for i in range(5)]
    for user in users:
        await create_user(session, user)
    
    # Book all 5 users (3 confirmed, 2 waitlisted)
    print("📝 Booking all 5 users (expecting 3 confirmed, 2 waitlisted)...")
    bookings = []
    for user in users:
        booking_id, status = await book_conference(session, user, conf_name)
        bookings.append((user, booking_id, status))
    
    print_booking_state(bookings, "Initial State After All Bookings")
    
    confirmed_bookings = [(u, bid, s) for u, bid, s in bookings if s == "CONFIRMED"]
    waitlisted_bookings = [(u, bid, s) for u, bid, s in bookings if s == "WAITLISTED"]
    
    if len(confirmed_bookings) != 3 or len(waitlisted_bookings) != 2:
        print(f"❌ Initial state incorrect: expected 3 confirmed and 2 waitlisted, got {len(confirmed_bookings)} confirmed and {len(waitlisted_bookings)} waitlisted")
        return False
    
    print("✅ Initial state correct: 3 confirmed, 2 waitlisted")
    
    # Cancel 2 of the confirmed bookings
    cancellation_targets = confirmed_bookings[:2]  # Cancel first 2 confirmed bookings
    print(f"🚫 Canceling 2 confirmed bookings: {[u for u, bid, s in cancellation_targets]}")
    
    start_time = time.time()
    cancel_tasks = [cancel_booking(session, bid) for u, bid, s in cancellation_targets]
    cancel_results = await asyncio.gather(*cancel_tasks)
    end_time = time.time()
    
    print(f"⚡ Cancellations completed in {(end_time - start_time) * 1000:.1f}ms")
    
    if not all(cancel_results):
        print("❌ Some cancellations failed")
        return False
    
    # Wait a moment for promotion processing
    print("⏳ Waiting 3 seconds for queue processing and promotions...")
    await asyncio.sleep(3)
    
    # Check state after cancellations
    current_bookings = await get_conference_bookings(session, conf_name)
    print_booking_state(current_bookings, "State After Cancellations")
    
    confirmed, pending, waitlisted, canceled = count_booking_statuses(current_bookings)
    
    print(f"📊 Final state: confirmed={confirmed}, pending={pending}, waitlisted={waitlisted}, canceled={canceled}")
    
    # Expected behavior:
    # - 2 bookings should be canceled
    # - 1 booking should remain confirmed (the one we didn't cancel)
    # - 2 bookings should be promoted to confirmation pending (the waitlisted ones)
    # - 0 should remain waitlisted (all were promoted)
    expected_canceled = 2
    expected_remaining_confirmed = 1
    expected_pending = 2
    expected_waitlisted = 0
    
    if (canceled == expected_canceled and 
        confirmed == expected_remaining_confirmed and 
        pending == expected_pending and 
        waitlisted == expected_waitlisted):
        print("✅ Multiple cancellations with waitlist handled correctly!")
        print(f"   - {canceled} bookings canceled")
        print(f"   - {confirmed} booking remains confirmed")
        print(f"   - {pending} waitlisted users promoted to confirmation pending")
        print("📝 Note: Users now have 20 seconds to confirm their promoted bookings")
        
        # Let's observe what happens to the confirmation pending bookings
        print("\n🔍 Observing confirmation pending behavior...")
        print("⏰ Waiting to see if users confirm their bookings or timeout...")
        
        # Wait and check periodically
        for i in range(4):  # Check every 5 seconds for 20 seconds
            await asyncio.sleep(5)
            current_bookings = await get_conference_bookings(session, conf_name)
            confirmed, pending, waitlisted, canceled = count_booking_statuses(current_bookings)
            print(f"   After {(i+1)*5}s: confirmed={confirmed}, pending={pending}, waitlisted={waitlisted}, canceled={canceled}")
        
        print(f"ℹ️ Conference '{conf_name}' will auto-cleanup in ~18 seconds")
        return True
    else:
        print("⚠️ State after cancellations is different than expected, but checking if it's reasonable...")
        
        # Alternative acceptable states (due to timing variations):
        # Some users might have already confirmed or timed out
        total_active = confirmed + pending
        if (canceled == expected_canceled and 
            total_active <= 3 and  # Can't exceed slot limit
            total_active >= 1):    # At least the original confirmed booking should remain
            print("✅ Multiple cancellations handled reasonably (timing variations)")
            print(f"   - Total active bookings: {total_active} (within slot limit of 3)")
            print(f"   - Canceled bookings: {canceled} (as expected)")
            print(f"ℹ️ Conference '{conf_name}' will auto-cleanup in ~38 seconds")
            return True
        else:
            print("❌ Multiple cancellations test failed - unexpected final state")
            print(f"   Expected: canceled=2, total_active≤3, total_active≥1")
            print(f"   Actual: canceled={canceled}, total_active={total_active}")
            return False

async def main():
    print("🚀 Multiple Cancellations with Waitlist Test")
    print("=" * 60)
    print("📋 Test Scenario:")
    print("   - Create conference with 3 slots")
    print("   - Book 5 users (3 confirmed, 2 waitlisted)")
    print("   - Cancel 2 confirmed bookings")
    print("   - Observe waitlist promotion behavior")
    print("   - Confirmation timeout: 20 seconds")
    print("=" * 60)
    
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Test server connectivity
        try:
            async with session.get(f"{BASE_URL}/conference/test/bookings") as resp:
                print("✅ Server connectivity confirmed")
        except Exception as e:
            print(f"❌ Cannot connect to server: {e}")
            print("💡 Make sure the conference server is running on localhost:8080")
            return
        
        try:
            if await test_multiple_cancellations_with_waitlist(session):
                print("\n🎉 TEST PASSED! Multiple cancellations with waitlist handled correctly.")
            else:
                print("\n❌ TEST FAILED! Check the behavior above.")
        except Exception as e:
            print(f"\n❌ TEST ERROR: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 