import requests
import csv
import json


def view_all_shows(limit=5, offset=0, sort_by='ASC'):
    response = requests.get(f'http://127.0.0.1:5000/viewAllShows?limit={limit}&offset={offset}&sortBy={sort_by}')
    if response.status_code == 200:
        return response.json()


def transform_shows(data):
    l = []
    for a in data:
        d = {"Title": a[0], "Director": a[1], "Rating": a[2], "Release Year": a[3], "Country": a[4]}
        l.append(d)
    return l


def load_into_csv(data, output_filename):
    headers = ["Title", "Director", "Rating", "Release Year", "Country"]
    with open(output_filename, mode='w',encoding='UTF-8') as out_csv:
        writer = csv.DictWriter(out_csv, fieldnames=headers)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def load_into_json(data, output_filename):
    with open(output_filename, 'w') as f:
        json.dump(data, f)


data = view_all_shows(200, 0, 'ASC')
transformed_data = transform_shows(data)
print(transformed_data)
load_into_csv(transformed_data,'movies.csv')
load_into_json(transformed_data, 'movies.json')