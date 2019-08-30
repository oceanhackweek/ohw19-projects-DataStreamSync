from pushData2Db import Database
import os
import wave
import pandas as pd
import holoviews as hv
from holoviews import opts
import soundfile as sf
import numpy as np
import panel as pn
import hvplot.pandas
import hvplot.pandas
import scipy.signal
import matplotlib.pyplot as plt
import panel as pn
import panel.widgets as pnw
import cv2

def getDetecTimes(rowID,df):
    t1 = df.loc[rowID]['start_time_utc_unix']
    t2 = df.loc[rowID]['stop_time_utc_unix']
    return t1, t2

def getRecordID(db,tablename,t):
    cursor = db.connection.cursor()
    postgreSQL_select_Query = "select id from " + tablename + " where start_time_utc_unix < %s and stop_time_utc_unix > %s"
    cursor.execute(postgreSQL_select_Query, (t,t,))
    records = cursor.fetchall()
    return records[0][0]

def getRecord(db,tablename,ID):
    cursor = db.connection.cursor()
    postgreSQL_select_Query = "select * from " + tablename + " where id = %s"
    cursor.execute(postgreSQL_select_Query, (ID,))
    records = cursor.fetchall()
    wavfileinfo = {
            'dirpath': records[0][1],
            'fs_hz': records[0][3],
            'filename': records[0][2],
            'dur_sec': records[0][4],    
            'startdate_unix': records[0][5],
            'stoptdate_unix': records[0][6]           
            }
    return wavfileinfo

def getVideoRecord(db,tablename,ID):
    cursor = db.connection.cursor()
    postgreSQL_select_Query = "select * from " + tablename + " where id = %s"
    cursor.execute(postgreSQL_select_Query, (ID,))
    records = cursor.fetchall()
    fileinfo = {
            'dirpath': records[0][1],
            'fps': records[0][3],
            'filename': records[0][2],
            'dur_sec': records[0][4],    
            'startdate_unix': records[0][9],
            'stoptdate_unix': records[0][10]           
            }
    return fileinfo

def getSonarRecord(db,tablename,ID):
    cursor = db.connection.cursor()
    postgreSQL_select_Query = "select * from " + tablename + " where id = %s"
    cursor.execute(postgreSQL_select_Query, (ID,))
    records = cursor.fetchall()
    wavfileinfo = {
            'dirpath': records[0][1],
            'fps': records[0][3],
            'filename': records[0][2],
            'dur_sec': records[0][4],    
            'startdate_unix': records[0][9],
            'stoptdate_unix': records[0][10]           
            }
    return wavfileinfo

def getWaveform(info, t1,t2):
    filename=os.path.join(info['dirpath'],info['filename'])
    sig, fs = sf.read(filename, always_2d=True)
    dt1 = t1 - info['startdate_unix']
    dt2 = t2 - info['startdate_unix']
    dt1_n = int(np.floor(dt1*fs))
    dt2_n = int(np.ceil(dt2*fs)) 
    sig2 =sig[dt1_n:dt2_n]
    xaxis = np.arange(0,len(sig2),1/fs)   
    df  = pd.DataFrame(sig2)    
    return df, xaxis, sig[dt1_n:dt2_n], fs

def getSpectrogram(info, t1,t2):
    filename=os.path.join(info['dirpath'],info['filename'])
    sig, fs = sf.read(filename, always_2d=True)
    dt1 = t1 - info['startdate_unix'] -1
    dt2 = t2 - info['startdate_unix'] +1
    dt1_n = int(np.floor(dt1*fs))
    dt2_n = int(np.ceil(dt2*fs)) 
    sig2 =sig[dt1_n:dt2_n]    
    faxis, taxis, S = scipy.signal.spectrogram(sig2[:,0],fs)
    #df = pd.DataFrame({'x':taxis, 'y': taxis, 'S':S[:,:,0]})  
    df = 1
    return faxis, taxis, S, df
    
# GET AUDIO WAVEFOREM

def displayWaveform(rowID=1):
    rowID=int(rowID)
    # Open database
    db = Database()
    db.open()    
    # import detection table
    df = pd.read_sql('select * from audio_detections', con=db.connection)
    # Name of the table
    tablename = 'audio_data'
    # time of the detection
    t1, t2 = getDetecTimes(rowID,df)
    # ID of file for that detection
    ID = getRecordID(db,tablename,t1)
    # Get info for that record
    info = getRecord(db,tablename,ID)
    df2, xaxis, waveform, fs = getWaveform(info, t1,t2) 
    db.close()
    return df2.hvplot.line()

def displaySpectrogram(rowID=1):
    # Open database
    rowID=int(rowID)
    db = Database()
    db.open()
    # import detection table
    df = pd.read_sql('select * from audio_detections', con=db.connection)
    # Name of the table
    tablename = 'audio_data'
    # time of the detection
    t1, t2 = getDetecTimes(rowID,df)
    # ID of file for that detection
    ID = getRecordID(db,tablename,t1)
    # Get info for that record
    info = getRecord(db,tablename,ID)
    df2, xaxis, waveform, fs = getWaveform(info, t1,t2) 
    faxis, taxis, S, df3 = getSpectrogram(info, t1,t2)
    return hv.Image(S)

def displayVideo(rowID=1):
    # Open database
    rowID=int(rowID)
    db = Database()
    db.open()
    # import detection table
    df = pd.read_sql('select * from audio_detections', con=db.connection)
    # Name of the table
    tablename = 'video_data'
    # time of the detection
    t1, t2 = getDetecTimes(rowID,df)
    # ID of file for that detection
    ID = getRecordID(db,tablename,t1)
    # Get info for that record
    info = getVideoRecord(db,tablename,ID)
    #faxis, taxis, S, df3 = getVideoFrame(info, t1,t2)
    filename=os.path.join(info['dirpath'],info['filename'])
    frameTime = t1 - info['startdate_unix']
    video = cv2.VideoCapture(filename)
    video.set(0,frameTime*1000) # set pointer to requested frame time (in ms)
    grabbed, frame = video.read() 
    frame_GS = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    video.release()
    img = hv.Image(frame_GS)
    return hv.Layout([img.opts(cmap='gray',xaxis=None, yaxis=None)]) 

def displaySonar(rowID=1):    
    # Open database
    rowID=int(rowID)
    db = Database()
    db.open()
    # import detection table
    df = pd.read_sql('select * from audio_detections', con=db.connection)
    # Name of the table
    tablename = 'aris_data'
    # time of the detection
    t1, t2 = getDetecTimes(rowID,df)
    # ID of file for that detection
    ID = getRecordID(db,tablename,t1)
    # Get info for that record
    info = getSonarRecord(db,tablename,ID)
    filename=os.path.join(info['dirpath'],info['filename'])
    frameTime = t1 - info['startdate_unix']
    dt = t1 - info['startdate_unix']
    dt_n = int(np.round(dt/info['fps']))
    mat = scipy.io.loadmat(filename)
    frames = mat['Data']['acousticData'][0, 0]
    frame = np.transpose(frames[:,:,dt_n])
    img = hv.Image(frame)
    return hv.Layout([img.opts(cmap='kg',xaxis=None, yaxis=None)]) 


##faxis, taxis, S, df3 = getVideoFrame(info, t1,t2)
#filename=os.path.join(info['dirpath'],info['filename'])
#frameTime = t1 - info['startdate_unix']
#video = cv2.VideoCapture(filename)
#video.set(0,frameTime*1000) # set pointer to requested frame time (in ms)
#grabbed, frame = video.read() 
#frame_GS = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
#video.release()
#img = hv.Image(frame_GS)
##return hv.Layout([img.opts(cmap='gray',xaxis=None, yaxis=None)]) 


#dt_n = np.round(dt/info['fps'])
#mat = scipy.io.loadmat(filename)
#frames = mat['Data']['acousticData'][0, 0]
##    