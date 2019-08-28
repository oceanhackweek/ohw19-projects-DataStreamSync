# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 15:03:15 2019

This script creates the tables in the Postgres database

@author: xavier.mouy
"""

import psycopg2


class Database():
    def __init__(self):
        self.user = "postgres"
        self.password = "postgres"
        self.password = "postgres"
        self.host = "localhost"
        self.port = "5432"
        self.database = "DataStreamSync"
        self.isopen = False

    def open(self, verbose=False):
        if self.isopen is False:
            try:
                self.connection = psycopg2.connect(user=self.user,
                                                   password=self.password,
                                                   host=self.host,
                                                   port=self.port,
                                                   database=self.database)
                self.cursor = self.connection.cursor()
                self.isopen = True
                if verbose:
                    # Print PostgreSQL Connection properties
                    print(self.connection.get_dsn_parameters(), "\n")
                    # Print PostgreSQL version
                    self.cursor.execute("SELECT version();")
                    record = self.cursor.fetchone()
                    print("You are connected to - ", record, "\n")
            except (Exception, psycopg2.Error) as error:
                print("Error while connecting to PostgreSQL", error)
        else:
            print('Connection already open')

    def close(self, verbose=False):
        if(self.connection):
            self.cursor.close()
            self.connection.close()
            self.isopen = False
            if verbose:
                print("PostgreSQL connection is closed")

    def createTableAudioData(self, name):
        try:
            create_table_query = '''CREATE TABLE ''' + name + '''
                    (ID INT PRIMARY KEY     NOT NULL,
                     DIRPATH   TEXT   NOT NULL,
                     FILENAME  TEXT   NOT NULL,
                     FS        INT   NOT NULL,
                     DURATION  REAL   NOT NULL,
                     START_TIME_UTC_UNIX   REAL   NOT NULL,
                     STOP_TIME_UTC_UNIX    REAL   NOT NULL,
                     START_TIME_UTC   DATE   NOT NULL,
                     STOP_TIME_UTC    DATE   NOT NULL
                     ); '''
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)

    def createTableVideoData(self, name):
            try:
                create_table_query = '''CREATE TABLE ''' + name + '''
                        (ID INT PRIMARY KEY     NOT NULL,
                         DIRPATH   TEXT   NOT NULL,
                         FILENAME  TEXT   NOT NULL,
                         FPS        INT   NOT NULL,
                         DURATION  REAL   NOT NULL,
                         SIZE_X INT NOT NULL,
                         SIZE_Y INT NOT NULL,
                         RESOLUTION_X INT,
                         RESOLUTION_Y INT,
                         START_TIME_UTC_UNIX   REAL   NOT NULL,
                         STOP_TIME_UTC_UNIX    REAL   NOT NULL,
                         START_TIME_UTC   DATE   NOT NULL,
                         STOP_TIME_UTC    DATE   NOT NULL
                         ); '''
                self.cursor.execute(create_table_query)
                self.connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error while creating PostgreSQL table", error)

    def createTableAudioDetections(self, name):
        try:
            create_table_query = '''CREATE TABLE ''' + name + '''
                    (ID INT PRIMARY KEY     NOT NULL,
                     DIRPATH   TEXT   NOT NULL,
                     FILENAME  TEXT   NOT NULL,                    
                     DURATION  REAL   NOT NULL,
                     START_TIME_OFFSET REAL NOT NULL,
                     STOP_TIME_OFFSET REAL NOT NULL,                     
                     START_TIME_UTC_UNIX   REAL   NOT NULL,
                     STOP_TIME_UTC_UNIX    REAL   NOT NULL,
                     START_TIME_UTC   DATE   NOT NULL,
                     STOP_TIME_UTC    DATE   NOT NULL,
                     FREQ_MIN   REAL   NOT NULL,
                     FREQ_MAX   REAL   NOT NULL,
                     CLASS   TEXT   NOT NULL,
                     CLASS_CONFIDENCE   REAL   NOT NULL
                     ); '''
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)


db = Database()
db.open()
db.createTableAudioData('AUDIO_DATA')
db.createTableVideoData('VIDEO_DATA')
db.createTableVideoData('ARIS_DATA')
db.createTableAudioDetections('FISH_SOUND_DETECTIONS')
db.close()
