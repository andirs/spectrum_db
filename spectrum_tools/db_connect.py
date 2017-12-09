import datetime
import numpy as np
import pandas as pd
import json
import os
from sqlalchemy import create_engine

class SpectrumDB:
    """
    Helper class to connect to spectrum database
    and download articles and information.
    """
    def __init__(self):
        self.engine = None
        self.load_db_credentials()

    def load_db_credentials(self):
        # check for credentials
        if not os.path.exists('credentials.json'):
            raise ValueError("credentials.json is missing.")
        with open('credentials.json') as json_file:  
            credentials = json.load(json_file)
        self.username = credentials['username']
        self.password = credentials['password']
        self.url = credentials['url']
        self.port = credentials['port']
        self.dbname = credentials['dbname']
        self.sql_dialect = credentials['sql_dialect']
        self.sql_engine = credentials['sql_engine']

    def __check_engine(self, engine):
        if not engine and not self.engine:
            self.engine = self.connect_to_sql(self.sql_dialect, self.sql_engine, self.username,
                                              self.password, self.url, self.dbname, self.port)
        return self.engine

    def connect_to_sql(self, sql_dialect, sql_engine, username, password, url, dbname, port, verbose=False):
        """
        Helper method to connect to sql database using sqlalchemy.
        Method doesn't actually connect to a database
        but returns a database engine to conveniantly use pandas.read_sql_query().

        Returns
        -------
        SQLAlchemy Engine instance
        """
        port = "" if port is None else ":" + port
        engine_statement = sql_dialect + "+" if sql_dialect is not None else ""
        engine_statement += sql_engine + "://" + username + ":" + password + "@" + url + port + "/" + dbname
        if verbose:
            print(engine_statement)

        self.engine = create_engine(engine_statement)

        return self.engine

    def select_all_query_builder(self, table_name, limit):
        query = "SELECT * FROM {}".format(table_name)
        if limit:
            query += " LIMIT {}".format(limit)
        return query

    def get_last_feed_items(self, engine=None):
        engine = self.__check_engine(engine)

        start_date = datetime.datetime.today() - datetime.timedelta(weeks=8)
        end_date = datetime.datetime.today()

        query = """ 
            SELECT * 
            FROM feed_fetcher_feeditem 
            WHERE "publication_date" >= %(start_date)s 
            AND "publication_date" < %(end_date)s + interval '1 day' 
            AND "content" != ''
        """
        query_params = {'start_date': start_date, 'end_date': end_date}
        feed_df = pd.read_sql_query(query, con=engine, params=query_params)
        return feed_df

    def get_publications(self, engine=None, limit=100):
        engine = self.__check_engine(engine)
        query = self.select_all_query_builder("feed_fetcher_publication", limit=limit)
        pub_df = pd.read_sql_query(query, con=engine)
        return pub_df

    def get_feeds(self, engine=None, limit=100):
        engine = self.__check_engine(engine)
        query = self.select_all_query_builder("feed_fetcher_feed", limit=limit)
        pub_df = pd.read_sql_query(query, con=engine)
        return pub_df

    def get_tags(self, engine=None, limit=100):
        engine = self.__check_engine(engine)
        query = self.select_all_query_builder("feed_fetcher_tag", limit=limit)
        tag_df = pd.read_sql_query(query, con=engine)
        return tag_df

    def get_associations(self, engine=None, limit=100):
        engine = self.__check_engine(engine)
        query = self.select_all_query_builder("feed_fetcher_association", limit=limit)
        assoc_df = pd.read_sql_query(query, con=engine)
        return assoc_df

    def get_sql(self, query, engine=None):
        engine = self.__check_engine(engine)
        sql_df = pd.read_sql_query(query, con=engine)

        return sql_df