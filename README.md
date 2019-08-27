# ohw19-projects-DataStreamSync
Interface to sync and visualize data from several instruments

Team members: Xavier and Coral 

# Instruments and data

For prototyping purposes, this project will use data from the [Fish Acoustic Experiment](https://www.oceannetworks.ca/do-fish-talk-innovative-experiment-study-fish-using-sound-and-imaging) deployed on the Ocean Networks Canada (ONC) Venus observatory.

The platform has three types of instruments
- 1x ARIS High frequency sonar:  3D matrix as .mat file
- 1x Video camera: .mp4 files
- 1x omnidirectional hydrophone: .wav files

![](https://www.oceannetworks.ca/sites/default/files/images/posts/2017-04-Delta-site.jpg)

A basic automatic fish sound detector has be run through the hydrophone data. Results from this detector are in .mat files.

# Objective of this project:

The objective is to create an interface that allows to visualize the spectrogram and waveform of the fish sounds detected as well as the video and sonar data at the time of the detection.
As a starting point it will use data from the local computer. Future versions may include using the ONC API to get the data directly from their servers.

# Building blocks

## Back-end: 
- read data from all instruments (different formats) and integrate them in a Postgres database
- query tool to gather data from all instruments in the database for a given time stamp

## Front-end: 
build dashboard allowing to list fish detections (in a table), select them and display data from othe rinstrument for this time stamp.

Components:
- Table with selectable entries (i.e. fish detections time stamps, duration and frequency bounds)
- Waveform panel: displays the waveform of the detection from the hydrophone data
- Spectrogram panel: displays the spectrogram of the detection from the hydrophone data- 
- Video panel: plays video from the video camera. Only includes x seconds before and after the fish sound detected. Play/stop/pause control buttons 
- Sonar panel: plays video of the data from the sonar. Only includes x seconds before and after the fish sound detected. Play/stop/pause control buttons

# Potential tools:
- [psycopg](http://initd.org/psycopg/)
- [Panel](https://panel.pyviz.org/reference/index.html)
- [HoloViz](https://holoviz.org/tutorial/index.html)




