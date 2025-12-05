"""
Producer-Consumer Pattern Implementation
Demonstrates thread synchronization using Queue and threading
"""
import threading
import queue
import time
import random
from typing import List


class ProducerConsumer:
    """
    Implements producer-consumer pattern with thread synchronization.
    Producer reads from source container and places items in shared queue.
    Consumer reads from queue and stores items in destination container.
    """
    
    def __init__(self, source_data: List, max_queue_size: int = 10):
        """
        Initialize producer-consumer system.
        
        Args:
            source_data: List of items to be processed
            max_queue_size: Maximum size of the shared queue
        """
        self.source_container = source_data.copy()
        self.destination_container = []
        self.shared_queue = queue.Queue(maxsize=max_queue_size)
        self.producer_thread = None
        self.consumer_thread = None
        self.lock = threading.Lock()
        self.items_produced = 0
        self.items_consumed = 0
        
    def producer(self):
        """
        Producer thread function.
        Reads items from source container and puts them in shared queue.
        """
        print("[Producer] Starting production...")
        
        for item in self.source_container:
            # Simulate some processing time
            time.sleep(random.uniform(0.01, 0.1))
            
            # Put item in queue (blocks if queue is full)
            self.shared_queue.put(item)
            
            with self.lock:
                self.items_produced += 1
                print(f"[Producer] Produced: {item} | Queue size: {self.shared_queue.qsize()}")
        
        # Signal that production is complete by adding sentinel value
        self.shared_queue.put(None)
        print("[Producer] Production complete. Sentinel sent.")
    
    def consumer(self):
        """
        Consumer thread function.
        Reads items from shared queue and stores in destination container.
        """
        print("[Consumer] Starting consumption...")
        
        while True:
            # Get item from queue (blocks if queue is empty)
            item = self.shared_queue.get()
            
            # Check for sentinel value (None indicates end of production)
            if item is None:
                print("[Consumer] Received sentinel. Stopping consumption.")
                self.shared_queue.task_done()
                break
            
            # Simulate some processing time
            time.sleep(random.uniform(0.01, 0.15))
            
            # Store item in destination container
            with self.lock:
                self.destination_container.append(item)
                self.items_consumed += 1
                print(f"[Consumer] Consumed: {item} | Destination size: {len(self.destination_container)}")
            
            # Mark task as done
            self.shared_queue.task_done()
        
        print("[Consumer] Consumption complete.")
    
    def start(self):
        """
        Start producer and consumer threads.
        """
        print("=== Starting Producer-Consumer System ===\n")
        
        # Create and start threads
        self.producer_thread = threading.Thread(target=self.producer, name="ProducerThread")
        self.consumer_thread = threading.Thread(target=self.consumer, name="ConsumerThread")
        
        self.consumer_thread.start()
        self.producer_thread.start()
    
    def wait_completion(self):
        """
        Wait for both threads to complete execution.
        """
        # Wait for producer to finish
        if self.producer_thread:
            self.producer_thread.join()
        
        # Wait for consumer to finish
        if self.consumer_thread:
            self.consumer_thread.join()
        
        # Ensure queue is empty
        self.shared_queue.join()
        
        print("\n=== Producer-Consumer System Complete ===")
        self.print_statistics()
    
    def print_statistics(self):
        """
        Print execution statistics.
        """
        print(f"\nStatistics:")
        print(f"  Items Produced: {self.items_produced}")
        print(f"  Items Consumed: {self.items_consumed}")
        print(f"  Source Container Size: {len(self.source_container)}")
        print(f"  Destination Container Size: {len(self.destination_container)}")
        print(f"  Data Integrity: {'PASS' if self.source_container == self.destination_container else 'FAIL'}")


def main():
    """
    Main function to demonstrate producer-consumer pattern.
    """
    # Create source data
    source_data = [f"Item-{i}" for i in range(1, 21)]
    
    print("Source Data:", source_data)
    print()
    
    # Create and run producer-consumer system
    pc_system = ProducerConsumer(source_data, max_queue_size=5)
    pc_system.start()
    pc_system.wait_completion()
    
    print("\nDestination Data:", pc_system.destination_container)


if __name__ == "__main__":
    main()