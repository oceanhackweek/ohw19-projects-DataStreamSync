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
import cv2
import scipy.io
import numpy as np

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
    #def listTables(self):
#        conn = psycopg2.connect(conn_string)
#        cursor = conn.cursor()
#        cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
#        print cursor.fetchall()

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
                    (ID SERIAL PRIMARY KEY ,
                     DIRPATH   TEXT   NOT NULL,
                     FILENAME  TEXT   NOT NULL,
                     FS        INT   NOT NULL,
                     DURATION  REAL   NOT NULL,
                     START_TIME_UTC_UNIX   DOUBLE PRECISION   NOT NULL,
                     STOP_TIME_UTC_UNIX    DOUBLE PRECISION   NOT NULL,
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
                        (ID SERIAL PRIMARY KEY ,
                         DIRPATH   TEXT   NOT NULL,
                         FILENAME  TEXT   NOT NULL,
                         FPS        INT   NOT NULL,
                         DURATION  REAL   NOT NULL,
                         SIZE_X INT NOT NULL,
                         SIZE_Y INT NOT NULL,
                         RESOLUTION_X INT,
                         RESOLUTION_Y INT,
                         START_TIME_UTC_UNIX   DOUBLE PRECISION   NOT NULL,
                         STOP_TIME_UTC_UNIX    DOUBLE PRECISION   NOT NULL,
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
                    (ID SERIAL PRIMARY KEY,
                     DIRPATH   TEXT   NOT NULL,
                     FILENAME  TEXT   NOT NULL,
                     DURATION  REAL   NOT NULL,
                     START_TIME_OFFSET REAL NOT NULL,
                     STOP_TIME_OFFSET REAL NOT NULL,
                     START_TIME_UTC_UNIX   DOUBLE PRECISION   NOT NULL,
                     STOP_TIME_UTC_UNIX    DOUBLE PRECISION   NOT NULL,
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

    def addAudioFile2Db(self, wavfileInfo):
        try:            
            postgres_insert_query = """ INSERT INTO audio_data (DIRPATH, FILENAME, FS, DURATION, start_time_utc_unix, stop_time_utc_unix, start_time_utc, stop_time_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (wavfileInfo['dirpath'], wavfileInfo['filename'],wavfileInfo['fs_hz'],wavfileInfo['dur_sec'], wavfileInfo['startdate_unix'],  wavfileInfo['stoptdate_unix'], wavfileInfo['startdate_obj'], wavfileInfo['stoptdate_obj'])
            self.cursor.execute(postgres_insert_query, record_to_insert)
            self.connection.commit()
            count = self.cursor.rowcount
            #print (count, "Record inserted successfully into table")
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)
    
    def addVideoFile2Db(self, videofileInfo):
        try:            
            postgres_insert_query = """ INSERT INTO video_data (DIRPATH, FILENAME, FPS, DURATION, SIZE_X, SIZE_Y, start_time_utc_unix, stop_time_utc_unix, start_time_utc, stop_time_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (videofileInfo['dirpath'], videofileInfo['filename'],videofileInfo['fps'],videofileInfo['dur_sec'], videofileInfo['width'], videofileInfo['height'], videofileInfo['startdate_unix'],  videofileInfo['stoptdate_unix'], videofileInfo['startdate_obj'], videofileInfo['stoptdate_obj'])
            self.cursor.execute(postgres_insert_query, record_to_insert)
            self.connection.commit()
            count = self.cursor.rowcount
            #print (count, "Record inserted successfully into table")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)

    def addSonarFile2Db(self, sonarfileInfo):
        try:            
            postgres_insert_query = """ INSERT INTO aris_data (DIRPATH, FILENAME, FPS, DURATION, SIZE_X, SIZE_Y, start_time_utc_unix, stop_time_utc_unix, start_time_utc, stop_time_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (sonarfileInfo['dirpath'], sonarfileInfo['filename'],sonarfileInfo['fps'],sonarfileInfo['dur_sec'],sonarfileInfo['width'], sonarfileInfo['height'], sonarfileInfo['startdate_unix'],  sonarfileInfo['stoptdate_unix'], sonarfileInfo['startdate_obj'], sonarfileInfo['stoptdate_obj'])
            self.cursor.execute(postgres_insert_query, record_to_insert)
            self.connection.commit()
            count = self.cursor.rowcount
            #print (count, "Record inserted successfully into table")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)

    def addDetection2Db(self, detecInfo):
        try:            
            try:
                postgres_insert_query = """ INSERT INTO audio_detections (DIRPATH, FILENAME, DURATION, start_time_offset, stop_time_offset, start_time_utc_unix, stop_time_utc_unix, start_time_utc, stop_time_utc, freq_min, freq_max, class, class_confidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                record_to_insert = (detecInfo['dirpath'], detecInfo['filename'],detecInfo['dur_sec'],detecInfo['t1_offset'], detecInfo['t2_offset'], detecInfo['startdate_unix'],  detecInfo['stoptdate_unix'], detecInfo['startdate_obj'], detecInfo['stoptdate_obj'],detecInfo['fmin'], detecInfo['fmax'],detecInfo['class'],detecInfo['confidence'])
                self.cursor.execute(postgres_insert_query, record_to_insert)
                self.connection.commit()
                count = self.cursor.rowcount
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error while creating PostgreSQL table", error)    
        except:
            print('Error - Detection not imported to DB')
           
    def addAudioDataset2Db(self, datadir):
        for path, subdirs, files in os.walk(datadir):
            for name in files:
                if name.lower().endswith('.wav'):
                    print(name)
                    wavfileInfo = getWavfileInfo(path, name)
                    db.addAudioFile2Db(wavfileInfo)

    def addVideoDataset2Db(self, datadir):
        for path, subdirs, files in os.walk(datadir):
            for name in files:
                if name.lower().endswith('.mp4'):
                    print(name)
                    videofileInfo = getVideofileInfo(path, name)
                    db.addVideoFile2Db(videofileInfo)

    def addSonarDataset2Db(self, datadir):
        for path, subdirs, files in os.walk(datadir):
            for name in files:
                if name.lower().endswith('.mat'):
                    print(name)
                    sonarfileinfo = getSonarfileInfo(path, name)
                    db.addSonarFile2Db(sonarfileinfo)
    
    def addAudioDetections2Db(self, detecdir):        
        for path, subdirs, files in os.walk(detecdir):    
            for name in files:
                if name.lower().endswith('.mat'):
                    print(name)
                    try:
                        fullfilename = os.path.join(path, name)
                        FishClassID = 3 # hard coded for now
                        # Load mat file
                        mat = scipy.io.loadmat(fullfilename)            
                        # Nb of detections
                        nDetec = mat['Detec']['predClass'].shape[1]
                        wavfile = mat['infile'][0][:]
                        regex = r'\d{8}T\d{6}.\d{3}Z'
                        datetimeformat = '%Y%m%dT%H%M%S.%fZ' 
                        fileStartTime=filename2date(wavfile, regex, datetimeformat)
                        for idx in range(1,nDetec):                        
                            classID=mat['Detec']['predClass'][0][idx][0][0]
                            if classID == FishClassID:
                                classConf=mat['Detec']['confidence'][0][idx][0][FishClassID-1]
                                t1_offset = mat['Detec']['t1'][0][idx][0][0]
                                t2_offset = mat['Detec']['t2'][0][idx][0][0]
                                fmin = mat['Detec']['fmin'][0][idx][0][0]
                                fmax = mat['Detec']['fmax'][0][idx][0][0]
                                t1_utc = fileStartTime + datetime.timedelta(seconds=t1_offset)
                                t2_utc = fileStartTime + datetime.timedelta(seconds=t2_offset)
                                t1_utc_unix = datetime2unix(t1_utc)
                                t2_utc_unix = datetime2unix(t2_utc)                    
                                detecInfo = {
                                    'dirpath': path,
                                    'filename': name,
                                    'dur_sec': t2_offset-t1_offset,
                                    't1_offset': t1_offset,
                                    't2_offset': t2_offset,
                                    'startdate_obj': t1_utc,
                                    'stoptdate_obj': t2_utc,
                                    'startdate_unix': t1_utc_unix,
                                    'stoptdate_unix': t2_utc_unix,
                                    'fmin': fmin,
                                    'fmax': fmax,
                                    'class': 'Fish',
                                    'confidence': classConf
                                    }
                                db.addDetection2Db(detecInfo)
                    except:
                        print('Error - file not imported to DB')


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


def getVideofileInfo(path, name):
    fullfilename = os.path.join(path, name)
    camera = cv2.VideoCapture(fullfilename)
    length = int(camera.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(camera.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(camera.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(camera.get(cv2.CAP_PROP_FPS))
    dur = length/fps
    camera.release()
    # start and stop dates/times - python datetime objects
    regex = r'\d{8}T\d{6}.\d{3}Z'
    datetimeformat = '%Y%m%dT%H%M%S.%fZ'
    startdate = filename2date(name, regex, datetimeformat)
    stopdate = startdate + datetime.timedelta(seconds=dur)
    # convert to date and times to Unix time
    startdateunix = datetime2unix(startdate)
    stoptdateunix = datetime2unix(stopdate)
    # package in dictionary
    videofileinfo = {
            'dirpath': path,
            'filename': name,
            'dur_sec': dur,
            'fps': fps,
            'width': width,
            'height': height,
            'startdate_obj': startdate,
            'stoptdate_obj': stopdate,
            'startdate_unix': startdateunix,
            'stoptdate_unix': stoptdateunix
        }
    return videofileinfo


def getSonarfileInfo(path, name):
    fullfilename = os.path.join(path, name)
    # Load mat file
    mat = scipy.io.loadmat(fullfilename)
    frames = mat['Data']['acousticData'][0, 0]
    # get start and stop date/time
    datetimeformat = '%Y%m%dT%H%M%S.%fZ'
    startdate = datetime.datetime.strptime(mat['Data']['startTime'][0, 0][0], datetimeformat)
    stopdate = datetime.datetime.strptime(mat['Data']['endTime'][0, 0][0], datetimeformat)
    # convert to date and times to Unix time
    startdateunix = datetime2unix(startdate)
    stoptdateunix = datetime2unix(stopdate)
    dur = stoptdateunix - startdateunix
    fps = np.round(frames.shape[-1]/dur)
    nbinsRange = frames.shape[1]
    nbinsAngle = frames.shape[0]
    sonarfileinfo = {
                        'dirpath': path,
                        'filename': name,
                        'dur_sec': dur,
                        'fps': fps,
                        'width': nbinsRange,
                        'height': nbinsAngle,
                        'startdate_obj': startdate,
                        'stoptdate_obj': stopdate,
                        'startdate_unix': startdateunix,
                        'stoptdate_unix': stoptdateunix
                    }
    return sonarfileinfo

def main():
    # TO DO ######################################################################
        # - Check if tables already exist before creating them
        # - check that you can't duplicate entries
    ##############################################################################
    
    # Open connection to posgres database
    db = Database()
    db.open()
    
    # Scan through audio files and populate posgres table
    wavdir = r'J:\ONC_FAE\data\hydrophone\dev'
    db.createTableAudioData('AUDIO_DATA')
    db.addAudioDataset2Db(wavdir)
    
    # Scan through video files and populate posgres table
    videodir = r'J:\ONC_FAE\data\camera\data\dev'
    db.createTableVideoData('VIDEO_DATA')
    db.addVideoDataset2Db(videodir)
    
    # Scan through sonar (.mat) files and populate posgres table
    sonardir = r'J:\ONC_FAE\data\sonar\dev'
    db.createTableVideoData('ARIS_DATA')
    db.addSonarDataset2Db(sonardir)
    
    # Scan through audio detections (.mat) files and populate posgres table
    detecdir = r'J:\ONC_FAE\detections\hydrophone\20180329\raw_detections\dev'
    db.createTableAudioDetections('AUDIO_DETECTIONS')
    db.addAudioDetections2Db(detecdir)
    
    # close connection to database
    db.close()

if __name__ == "__main__":
    main()