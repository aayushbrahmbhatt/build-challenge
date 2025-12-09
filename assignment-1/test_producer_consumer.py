"""
Unit tests for Producer-Consumer implementation
"""
import unittest
import time
from producer_consumer import ProducerConsumer


class TestProducerConsumer(unittest.TestCase):
    """
    Test cases for ProducerConsumer class.
    """
    
    def test_basic_functionality(self):
        """
        Test basic producer-consumer functionality with small dataset.
        """
        source_data = ["A", "B", "C", "D", "E"]
        pc = ProducerConsumer(source_data, max_queue_size=3)
        
        pc.start()
        pc.wait_completion()
        
        # Verify all items transferred
        self.assertEqual(len(pc.destination_container), len(source_data))
        self.assertEqual(pc.items_produced, len(source_data))
        self.assertEqual(pc.items_consumed, len(source_data))
        
        # Verify data integrity
        self.assertEqual(source_data, pc.destination_container)
    
    def test_empty_source(self):
        """
        Test behavior with empty source container.
        """
        source_data = []
        pc = ProducerConsumer(source_data, max_queue_size=5)
        
        pc.start()
        pc.wait_completion()
        
        self.assertEqual(len(pc.destination_container), 0)
        self.assertEqual(pc.items_produced, 0)
        self.assertEqual(pc.items_consumed, 0)
    
    def test_single_item(self):
        """
        Test with single item in source.
        """
        source_data = ["SingleItem"]
        pc = ProducerConsumer(source_data, max_queue_size=1)
        
        pc.start()
        pc.wait_completion()
        
        self.assertEqual(len(pc.destination_container), 1)
        self.assertEqual(pc.destination_container[0], "SingleItem")
    
    def test_large_dataset(self):
        """
        Test with larger dataset to verify thread synchronization.
        """
        source_data = list(range(1, 101))
        pc = ProducerConsumer(source_data, max_queue_size=10)
        
        pc.start()
        pc.wait_completion()
        
        # Verify all items transferred
        self.assertEqual(len(pc.destination_container), 100)
        self.assertEqual(pc.items_produced, 100)
        self.assertEqual(pc.items_consumed, 100)
        
        # Verify order and integrity
        self.assertEqual(source_data, pc.destination_container)
    
    def test_different_data_types(self):
        """
        Test with mixed data types.
        """
        source_data = [1, "text", 3.14, True]
        pc = ProducerConsumer(source_data, max_queue_size=5)
        
        pc.start()
        pc.wait_completion()
        
        self.assertEqual(len(pc.destination_container), len(source_data))
        self.assertEqual(source_data, pc.destination_container)
    
    def test_queue_blocking_behavior(self):
        """
        Test that queue properly blocks when full.
        """
        source_data = list(range(1, 21))
        pc = ProducerConsumer(source_data, max_queue_size=2)
        
        pc.start()
        
        # Allow some time for threads to run
        time.sleep(0.5)
        
        # Queue should have items during processing
        self.assertTrue(pc.shared_queue.qsize() <= 2)
        
        pc.wait_completion()
        
        # Queue should be empty after completion
        self.assertEqual(pc.shared_queue.qsize(), 0)
    
    def test_thread_safety(self):
        """
        Test thread safety with concurrent access to shared resources.
        """
        source_data = list(range(1, 51))
        pc = ProducerConsumer(source_data, max_queue_size=5)
        
        pc.start()
        pc.wait_completion()
        
        # No items should be lost or duplicated
        self.assertEqual(set(source_data), set(pc.destination_container))
        self.assertEqual(len(source_data), len(pc.destination_container))


if __name__ == "__main__":
    unittest.main()