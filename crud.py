import psycopg2
import configparser
from google.cloud.sql.connector import Connector
import sqlalchemy
import pymysql

# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = connector.connect(
        "project:region:instance",
        "pymysql",
        user="root",
        password="qwerty",
        db="movies"
    )
    return conn

# create connection pool

#config = configparser.ConfigParser()
#config.read('config.ini')

#DATABASE = "movies"
#USER = "root"
#PASSWORD = "qwerty"
#HOST = "netflix-352919:us-central1:netflix2"
#PORT =  "5432"


class ShowsDBCRUD:
    def __init__(self):
        pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        )
        #self.con = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)

    def view_all_shows(self, limit, offset, sort_by):
        pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        )
        print('view_all_shows crud.py')
        with pool.connect() as db_conn:
        #cur = self.con.cursor()
            db_conn.execute(f"""
            SELECT
                s.title,
                s.director,
                s.rating,
                s.release_year,
                s.country
            FROM movies s
            ORDER BY s.release_year {sort_by}
            LIMIT {limit}
            OFFSET {offset};
            """)
            rows = db_conn()
            return rows

    def search_shows_by_title(self, search_phrase):
        print('search_shows_by_title crud.py')
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT
                *
            FROM movies s
            WHERE s.title LIKE '%{search_phrase}%';
            """)
        rows = cur.fetchall()
        print(rows)
        return rows

    def filter_shows_by_country(self, country, limit, offset):
        print('filter by country crud.py')
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT
                *
            FROM movies s
            WHERE s.country LIKE '%{country}%'
            LIMIT {limit}
            OFFSET {offset};
            """)
        rows = cur.fetchall()
        print(rows)
        return rows

    def filter_shows_by_rating(self, rating, limit, offset):
        print('filter by rating crud.py')
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT
                *
            FROM movies s
            WHERE s.rating = '{rating}'
            LIMIT {limit}
            OFFSET {offset};
            """)
        rows = cur.fetchall()
        print(rows)
        return rows

    def filter_shows_by_release_year(self, release_year, limit, offset):
        print('filter by country crud.py')
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT
                *
            FROM movies s
            WHERE s.release_year = {release_year}
            LIMIT {limit}
            OFFSET {offset};
            """)
        rows = cur.fetchall()
        print(rows)
        return rows

    def update_show_type(self, show_type, show_id):
        print('update_show_type crud.py')
        try:
            cur = self.con.cursor()
            cur.execute(f"""
                UPDATE movies
                SET type = '{show_type}'
                WHERE show_id = {show_id};
                """)
            self.con.commit()
        except:
            self.con.rollback()
            print("already modified..")

    def delete_show_by_id(self, show_id):
        print('delete_show_by_id crud.py')
        try:
            cur = self.con.cursor()
            cur.execute(f"""
                DELETE FROM movies
                WHERE show_id = {show_id};
                """)
            self.con.commit()
        except:
            self.con.rollback()
            print("already deleted..")

    def insert_show(self, show_id, type, title, director, cast, country,
                    date_added, release_year, rating, duration, listed_in, description):
        print('insert_show crud.py')
        try:
            cur = self.con.cursor()
            cur.execute(f"""
                INSERT INTO movies (show_id, type, title, director, "cast", country,
                 date_added, release_year, rating, duration, listed_in, description)
                VALUES ('{show_id}', '{type}', '{title}', '{director}', '{cast}', '{country}',
                 '{date_added}', {release_year}, '{rating}', '{duration}', '{listed_in}', '{description}');
                """)
            self.con.commit()
        except:
            self.con.rollback()
            print("already exists..")