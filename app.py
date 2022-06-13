from flask import Flask, request, render_template
from database import ShowsData

app = Flask(__name__)
database = ShowsData()

@app.route('/')
def index():
    return 'Welcome to the Netflix!'

@app.route('/viewAllShows', methods=['GET'])
def view_all_shows():
    args = request.args
    acceptable_args = {"offset", "limit", "sortBy"}
    for t, v in args.items():
        if t not in acceptable_args:
            return 'bad request!', 400
    print('view all shows app.py')
    data = database.view_all_shows(args)
    return data

@app.route('/searchShowsByTitle/<search_phrase>', methods=['GET'])
def search_shows_by_title(search_phrase):
    print('search shows by title app.py', search_phrase)
    data = database.search_shows_by_title(search_phrase)
    return data

@app.route('/filterShows', methods=['GET'])
def filter_shows():
    args = request.args
    acceptable_args = {"country", "release_year", "rating", "offset", "limit"}
    for t, v in args.items():
        if t not in acceptable_args:
            return 'bad request!', 400
    print('filter shows app.py', args)
    data = database.filter_shows(args)
    return data

@app.route('/updateShowType', methods=['PUT'])
def update_show_type():
    args = request.args
    acceptable_args = {"show_type", "show_id"}
    for t, v in args.items():
        if t not in acceptable_args:
            return 'bad request!', 400
    print('update show type app.py', args)
    database.update_show_type(args)
    return f'Show type modified for {args.get("show_id")}', 201

@app.route('/deleteShowById', methods=['DELETE'])
def delete_show_by_id():
    args = request.args
    acceptable_args = {"show_id"}
    for t, v in args.items():
        if t not in acceptable_args:
            return 'bad request!', 400
    print('delete show by id app.py', args)
    database.delete_show_by_id(args)
    return f'Deleted show id: {args.get("show_id")}', 201

@app.route('/insertShow', methods=['POST'])
def insert_show():
    args = request.args
    acceptable_args = {"show_id", "type", "title", "director",
                       "cast", "country", "date_added", "release_year",
                       "rating", "duration", "listed_in", "description"}
    for t, v in args.items():
        if t not in acceptable_args:
            return 'bad request!', 400
    print('insert show app.py', args)
    database.insert_show(args)
    return f'Show inserted: {args.get("show_id")}', 201
