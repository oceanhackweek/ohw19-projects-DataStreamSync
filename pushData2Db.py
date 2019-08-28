# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 15:03:15 2019

This script creates the tables in the Postgres database

@author: xavier.mouy
"""

import psycopg2
import os
import wave
import re
import datetime

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
                    (ID SERIAL PRIMARY KEY  NOT NULL,
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

    def addAudioData2Db(self, wavfileInfo):
        try:            
            postgres_insert_query = """ INSERT INTO audio_data (DIRPATH, FILENAME, FS, DURATION, start_time_utc_unix, stop_time_utc_unix, start_time_utc, stop_time_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (wavfileInfo['dirpath'], wavfileInfo['filename'],wavfileInfo['fs_hz'],wavfileInfo['dur_sec'], wavfileInfo['startdate_unix'],  wavfileInfo['stoptdate_unix'], wavfileInfo['startdate_obj'], wavfileInfo['stoptdate_obj'])
            self.cursor.execute(postgres_insert_query, record_to_insert)
            self.connection.commit()
            count = self.cursor.rowcount
            print (count, "Record inserted successfully into mobile table")
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)
        

#def getDateFromFile()
db = Database()
db.open()
#db.createTableAudioData('AUDIO_DATA')
#db.createTableVideoData('VIDEO_DATA')
#db.createTableVideoData('ARIS_DATA')
#db.createTableAudioDetections('FISH_SOUND_DETECTIONS')
#db.close()



def datetime2unix(datetimeobj):
    return datetimeobj.replace(tzinfo=datetime.timezone.utc).timestamp()


def filename2date(filename, regex, datetimeformat):
    match = re.search(regex, filename)
    return datetime.datetime.strptime(match.group(), datetimeformat)


def getWavfileInfo(path, name):
    fullfilename = os.path.join(path, name)
    wavobj = wave.open(fullfilename, mode='rb')
    fs = wavobj.getframerate()
    dur = wavobj.getnframes() / fs
    # start and stop dates/times - python datetime objects
    regex = r'\d{8}T\d{6}.\d{3}Z'
    datetimeformat = '%Y%m%dT%H%M%S.%fZ'
    startdate = filename2date(name, regex, datetimeformat)
    stopdate = startdate + datetime.timedelta(seconds=dur)
    # convert to date and times to Unix time
    startdateunix = datetime2unix(startdate)
    stoptdateunix = datetime2unix(stopdate)
    # package in dictionary
    wavfileinfo = {
            'dirpath': path,
            'filename': name,
            'dur_sec': dur,
            'fs_hz': fs,
            'startdate_obj': startdate,
            'stoptdate_obj': stopdate,
            'startdate_unix': startdateunix,
            'stoptdate_unix': stoptdateunix
        }
    return wavfileinfo


indir = r'J:\ONC_FAE\data\hydrophone\dev'
for path, subdirs, files in os.walk(indir):
    for name in files:
        if name.lower().endswith('.wav'):
            print(name)
            wavfileInfo = getWavfileInfo(path, name)
            
            db.addAudioData2Db(wavfileInfo) # <<<< NEED TO FIX THIS Method
            
            
