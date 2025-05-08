import unittest
from gridflow.downloader import QueryHandler

class TestDownloader(unittest.TestCase):
    def test_query_handler_init(self):
        qh = QueryHandler()
        self.assertEqual(len(qh.nodes), 4)

if __name__ == '__main__':
    unittest.main()