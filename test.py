import requests
import unittest

class TestNetflixShowsAPI(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_view_all_shows(self):
        response = requests.get('http://127.0.0.1:5000/viewAllShows?limit=5&offset=1&sortBy=ASC')
        actual_length = len(response.json())
        expected_length = 5
        self.assertEqual(actual_length, expected_length)

    def test_create_new_show(self):
        url = "http://127.0.0.1:5000/insertShow"
        data = {"show_id": 11229988,
                "type": "Movie",
                "title": "Test1",
                "director": "Test1",
                "cast": "Test1",
                "country": "Test1",
                "date_added": "2021-02-08",
                "release_year": 2021,
                "rating": "PG",
                "duration": "Test1",
                "listed_in": "Test1",
                "description": "Test1"}
        response = requests.post(url, data=data)
        self.assertEqual(response.status_code,201)

    def test_modify_show(self):
        url = "http://127.0.0.1:5000/updateShowType"
        data = {"show_id": 11229988,
                "show_type": "TV Show"}
        response = requests.put(url, data=data)
        self.assertEqual(response.status_code, 201)

    def test_delete_show(self):
        url = "http://127.0.0.1:5000/deleteShowById"
        data = {"show_id": 11229988}
        response = requests.delete(url, data=data)
        self.assertEqual(response.status_code, 201)

if __name__ == '__main__':
    unittest.main()
