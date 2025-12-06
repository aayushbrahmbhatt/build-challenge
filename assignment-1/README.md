# Build Challenge - Assignment 1: Producer-Consumer Pattern

## Overview
This implementation demonstrates the classic producer-consumer pattern with proper thread synchronization. The solution showcases concurrent programming principles, including thread-safe data structures, blocking queues, and proper synchronization mechanisms.

## Problem Description
The program simulates concurrent data transfer between:
- **Producer Thread**: Reads items from a source container and places them into a shared queue
- **Consumer Thread**: Retrieves items from the shared queue and stores them in a destination container

Both threads operate concurrently while maintaining data integrity through proper synchronization.

## Key Features
- Thread-safe implementation using blocking queues
- Proper synchronization with locks for shared resources
- Graceful shutdown mechanism using sentinel values (poison pill pattern)
- Comprehensive logging for monitoring thread activities
- Data integrity verification
- Configurable queue size to demonstrate blocking behavior

## Technologies Used
- **Python 3.7+**: `threading`, `queue` modules
- **unittest**: Python's built-in testing framework

---

## Implementation

### Prerequisites
- Python 3.7 or higher
- No external dependencies required (uses standard library)

### Project Structure
```
assignment-1/
├── producer_consumer.py          # Main implementation
├── test_producer_consumer.py     # Unit tests
└── README.md                      # This file
```

### Setup Instructions

#### Running the Application
```bash
# Run the main program
python producer_consumer.py

# Run unit tests
python -m unittest test_producer_consumer.py

# Run tests with verbose output
python -m unittest test_producer_consumer.py -v
```

### Sample Output
```
Source Data: ['Item-1', 'Item-2', 'Item-3', 'Item-4', 'Item-5', 'Item-6', 'Item-7', 'Item-8', 'Item-9', 'Item-10', 'Item-11', 'Item-12', 'Item-13', 'Item-14', 'Item-15', 'Item-16', 'Item-17', 'Item-18', 'Item-19', 'Item-20']

=== Starting Producer-Consumer System ===

[Producer] Starting production...
[Consumer] Starting consumption...
[Producer] Produced: Item-1 | Queue size: 1
[Producer] Produced: Item-2 | Queue size: 2
[Consumer] Consumed: Item-1 | Destination size: 1
[Producer] Produced: Item-3 | Queue size: 2
[Producer] Produced: Item-4 | Queue size: 3
[Consumer] Consumed: Item-2 | Destination size: 2
[Producer] Produced: Item-5 | Queue size: 3
[Producer] Produced: Item-6 | Queue size: 4
[Consumer] Consumed: Item-3 | Destination size: 3
[Producer] Produced: Item-7 | Queue size: 4
[Consumer] Consumed: Item-4 | Destination size: 4
[Producer] Produced: Item-8 | Queue size: 4
[Producer] Produced: Item-9 | Queue size: 5
[Consumer] Consumed: Item-5 | Destination size: 5
[Producer] Produced: Item-10 | Queue size: 5
[Consumer] Consumed: Item-6 | Destination size: 6
[Producer] Produced: Item-11 | Queue size: 5
[Consumer] Consumed: Item-7 | Destination size: 7
[Producer] Produced: Item-12 | Queue size: 5
[Consumer] Consumed: Item-8 | Destination size: 8
[Producer] Produced: Item-13 | Queue size: 5
[Consumer] Consumed: Item-9 | Destination size: 9
[Producer] Produced: Item-14 | Queue size: 5
[Consumer] Consumed: Item-10 | Destination size: 10
[Producer] Produced: Item-15 | Queue size: 5
[Consumer] Consumed: Item-11 | Destination size: 11
[Producer] Produced: Item-16 | Queue size: 5
[Consumer] Consumed: Item-12 | Destination size: 12
[Producer] Produced: Item-17 | Queue size: 5
[Consumer] Consumed: Item-13 | Destination size: 13
[Producer] Produced: Item-18 | Queue size: 5
[Consumer] Consumed: Item-14 | Destination size: 14
[Producer] Produced: Item-19 | Queue size: 5
[Consumer] Consumed: Item-15 | Destination size: 15
[Producer] Produced: Item-20 | Queue size: 5
[Producer] Production complete. Sentinel sent.
[Consumer] Consumed: Item-16 | Destination size: 16
[Consumer] Consumed: Item-17 | Destination size: 17
[Consumer] Consumed: Item-18 | Destination size: 18
[Consumer] Consumed: Item-19 | Destination size: 19
[Consumer] Consumed: Item-20 | Destination size: 20
[Consumer] Received sentinel. Stopping consumption.
[Consumer] Consumption complete.

=== Producer-Consumer System Complete ===

Statistics:
  Items Produced: 20
  Items Consumed: 20
  Source Container Size: 20
  Destination Container Size: 20
  Data Integrity: PASS

Destination Data: ['Item-1', 'Item-2', 'Item-3', 'Item-4', 'Item-5', 'Item-6', 'Item-7', 'Item-8', 'Item-9', 'Item-10', 'Item-11', 'Item-12', 'Item-13', 'Item-14', 'Item-15', 'Item-16', 'Item-17', 'Item-18', 'Item-19', 'Item-20']
```

### Test Output
```
$ python -m unittest test_producer_consumer.py -v

test_basic_functionality (__main__.TestProducerConsumer)
Test basic producer-consumer functionality with small dataset. ... ok
test_counters_accuracy (__main__.TestProducerConsumer)
Test that production and consumption counters are accurate. ... ok
test_different_data_types (__main__.TestProducerConsumer)
Test with mixed data types. ... ok
test_empty_source (__main__.TestProducerConsumer)
Test behavior with empty source container. ... ok
test_large_dataset (__main__.TestProducerConsumer)
Test with larger dataset to verify thread synchronization. ... ok
test_medium_dataset_with_small_queue (__main__.TestProducerConsumer)
Test medium dataset with very small queue. ... ok
test_multiple_sequential_runs (__main__.TestProducerConsumer)
Test multiple sequential runs to ensure no state leakage. ... ok
test_producer_faster_than_consumer (__main__.TestProducerConsumer)
Test scenario where producer is faster than consumer. ... ok
test_queue_blocking_behavior (__main__.TestProducerConsumer)
Test that queue properly blocks when full. ... ok
test_single_item (__main__.TestProducerConsumer)
Test with single item in source. ... ok
test_string_data_integrity (__main__.TestProducerConsumer)
Test with string data to ensure no string manipulation issues. ... ok
test_thread_safety (__main__.TestProducerConsumer)
Test thread safety with concurrent access to shared resources. ... ok

----------------------------------------------------------------------
Ran 12 tests in 3.847s

OK
```

---

## Implementation Details

### Synchronization Mechanisms

- **`queue.Queue`**: Thread-safe FIFO queue with blocking operations
- **`threading.Lock`**: Protects shared counters and containers
- **Sentinel Value (`None`)**: Signals end of production to consumer

### Thread Communication
1. **Producer** places items in the shared queue using blocking `put()` operation
2. **Consumer** retrieves items using blocking `take()` operation
3. Queue automatically handles synchronization - threads wait when queue is full/empty
4. Sentinel/poison pill signals completion without race conditions

### Data Integrity
- Order preservation: Items are consumed in the same order they're produced (FIFO)
- No data loss: All items from source appear in destination
- No duplicates: Each item processed exactly once
- Thread safety: Proper locking prevents race conditions

---

## Testing Objectives Met

### ✅ Thread Synchronization
- Demonstrates proper use of locks and synchronized blocks
- Prevents race conditions on shared resources
- Ensures atomic operations on counters

### ✅ Concurrent Programming
- Producer and consumer run in separate threads
- Threads operate concurrently without blocking each other unnecessarily
- Proper thread lifecycle management (start, join, terminate)

### ✅ Blocking Queues
- Uses bounded blocking queue with configurable size
- Demonstrates automatic blocking when queue is full/empty
- Shows producer-consumer coordination through queue operations

### ✅ Wait/Notify Mechanism
- Implicit in blocking queue operations
- Threads efficiently wait without busy-waiting
- Automatic signaling when queue state changes

---

## Test Coverage

### Unit Tests (12 Total)

#### 1. **test_basic_functionality**
- **Purpose**: Verifies core producer-consumer workflow
- **Test Data**: 5 items (A, B, C, D, E)
- **Validates**: Item transfer, counters, order preservation

#### 2. **test_empty_source**
- **Purpose**: Edge case handling with no input data
- **Test Data**: Empty list
- **Validates**: Graceful handling, no errors, zero counters

#### 3. **test_single_item**
- **Purpose**: Minimal dataset edge case
- **Test Data**: Single item
- **Validates**: Correct transfer with queue size 1

#### 4. **test_large_dataset**
- **Purpose**: Stress test for thread synchronization
- **Test Data**: 100 numeric items
- **Validates**: No race conditions, order preservation, all items transferred

#### 5. **test_different_data_types**
- **Purpose**: Type flexibility verification
- **Test Data**: Mixed types (int, string, float, bool, None, dict, list)
- **Validates**: Queue handles various Python objects

#### 6. **test_queue_blocking_behavior**
- **Purpose**: Verifies blocking queue prevents overflow
- **Test Data**: 20 items with max queue size 2
- **Validates**: Queue never exceeds max size, empties after completion

#### 7. **test_thread_safety**
- **Purpose**: Ensures no data corruption in concurrent access
- **Test Data**: 50 numeric items
- **Validates**: No lost items, no duplicates

#### 8. **test_producer_faster_than_consumer**
- **Purpose**: Tests rate mismatch handling
- **Test Data**: 30 items with small queue
- **Validates**: All items transfer despite speed differences

#### 9. **test_medium_dataset_with_small_queue**
- **Purpose**: Forces multiple blocking scenarios
- **Test Data**: 15 items with queue size 1
- **Validates**: Correct operation with minimal buffer

#### 10. **test_string_data_integrity**
- **Purpose**: Ensures no string manipulation issues
- **Test Data**: Greek alphabet strings
- **Validates**: String data remains unchanged

#### 11. **test_multiple_sequential_runs**
- **Purpose**: Tests independence of multiple executions
- **Test Data**: 3 items, run 3 times
- **Validates**: No state leakage between runs

#### 12. **test_counters_accuracy**
- **Purpose**: Validates thread-safe counter updates
- **Test Data**: 25 items
- **Validates**: Producer and consumer counters match actual transfers

### Test Statistics
- **Total Tests**: 12
- **Coverage Areas**: Thread synchronization, data integrity, edge cases, blocking behavior
- **Typical Runtime**: ~4 seconds
- **Success Rate**: 100% (all tests passing)

---

## Design Decisions

1. **Queue Size**: Default of 5-10 items demonstrates blocking without excessive memory
2. **Random Sleep Times**: Simulates real-world variable processing speeds
3. **Logging**: Comprehensive output for debugging and monitoring
4. **Statistics Tracking**: Validates correctness with production/consumption counts
5. **Poison Pill Pattern**: Clean shutdown without complex signaling

## Assumptions
- Items can be any data type (strings, numbers, objects)
- Processing order (FIFO) must be maintained
- System should handle graceful shutdown
- Performance is secondary to correctness and clarity

## Future Enhancements
- Multiple producer/consumer threads
- Priority queue implementation
- Bounded wait times with timeouts
- Metrics collection (throughput, latency)
- Configurable retry logic

---