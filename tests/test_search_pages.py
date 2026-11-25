import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import reading  # noqa: E402


class SearchPagesTest(unittest.TestCase):
    def fake(self, total):
        calls = []

        def fetch(limit, offset):
            calls.append((limit, offset))
            self.assertLessEqual(limit, 25)
            hits = [[{"id": str(i)}] for i in range(offset, min(offset + limit, total))]
            return {"total_results": total, "messages": hits}

        return fetch, calls

    def test_pages_past_25(self):
        fetch, calls = self.fake(100)
        data = reading._search_pages(fetch, 60)
        self.assertEqual(len(data["messages"]), 60)
        self.assertEqual(calls, [(25, 0), (25, 25), (10, 50)])

    def test_stops_at_total(self):
        fetch, calls = self.fake(30)
        data = reading._search_pages(fetch, 100)
        self.assertEqual(len(data["messages"]), 30)
        self.assertEqual(data["total_results"], 30)

    def test_small_limit_one_call(self):
        fetch, calls = self.fake(100)
        self.assertEqual(len(reading._search_pages(fetch, 5)["messages"]), 5)
        self.assertEqual(calls, [(5, 0)])


if __name__ == "__main__":
    unittest.main()
