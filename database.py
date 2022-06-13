import data as data
from crud import ShowsDBCRUD
from flask import jsonify


class ShowsData:
    def __init__(self):
        self.db = ShowsDBCRUD()

    def view_all_shows(self, args):
        print('view all_shows database.py')
        limit = args.get('limit')
        offset = args.get('offset')
        sort_by = args.get('sortBy')
        data = self.db.view_all_shows(limit, offset, sort_by)
        data = jsonify(data)
        print('here', data)
        return data

    def search_shows_by_title(self, search_phrase):
        print('search shows by title database.py', search_phrase)
        data = self.db.search_shows_by_title(search_phrase)
        data = jsonify(data)
        print(f'{data} {search_phrase}')
        return data

    def filter_shows(self, args):
        print('filter shows database.py', args)
        limit = args.get('limit')
        offset = args.get('offset')
        if args.get('country'):
            country = args.get('country')
            data = self.db.filter_shows_by_country(country, limit, offset)
        elif args.get('rating'):
            rating = args.get('rating')
            data = self.db.filter_shows_by_rating(rating, limit, offset)
        elif args.get('release_year'):
            release_year = args.get('release_year')
            data = self.db.filter_shows_by_rating(release_year, limit, offset)
        data = jsonify(data)
        return data

    def update_show_type(self, args):
        show_type = args.get('show_type')
        show_id = args.get('show_id')
        print('update show type database.py', show_type, show_id)
        data = self.db.update_show_type(show_type, show_id)
        data = jsonify(data)
        return data

    def insert_show(self, args):
        type = args.get('show_type')
        show_id = args.get('show_id')
        title = args.get('title')
        director = args.get('director')
        cast = args.get('cast')
        country = args.get('country')
        date_added = args.get('date_added')
        release_year = args.get('release_year')
        rating = args.get('rating')
        duration = args.get('duration')
        listed_in = args.get('listed_in')
        description = args.get('descripion')
        print('insert show database.py', show_id)
        data = self.db.insert_show( show_id, type, title, director, cast, country,date_added, release_year, rating, duration, listed_in, description)
        data = jsonify(data)
        return data

    def delete_show_by_id(self, args):
        show_type = args.get('show_type')
        show_id = args.get('show_id')
        print('delete show by id database.py', show_id)
        data = self.db.delete_show_by_id(show_id)
        data = jsonify(data)
        return data

   