# tests/test_system.py
import unittest
from core.system import SortingSystem

class TestSortingSystem(unittest.TestCase):
    def setUp(self):
        self.system = SortingSystem("config/test_config.json")

    def test_match_lane_ai_priority(self):
        ai_result = (1, "APPLE", 999)
        lane, status, tid = self.system.match_lane(None, ai_result, {"enable_ai": True, "ai_priority": True})
        self.assertEqual(lane, 1)
        self.assertTrue(status.startswith("AI_MATCHED"))