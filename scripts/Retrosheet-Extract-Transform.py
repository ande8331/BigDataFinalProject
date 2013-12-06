from os import listdir
from os.path import isfile, join
import re
import zipfile
import subprocess
import os

mypath = "."

fileList = [ f for f in listdir(mypath) if f.endswith(".zip") ]
for fname in fileList:
    #Unzip Files
    print "Extracting: " + fname
    zf = zipfile.ZipFile(fname, "r")
    zf.extractall(mypath)
    
    #Process AL
    with open(fname[0:4] + ".EVAC", 'a') as outfile:
        subprocess.call(["BEVENT", "-y", fname[0:4], "-f", "0-96", fname[0:4]+"*.eva"], stdout=outfile)
        
    #Process NL
    with open(fname[0:4] + ".EVNC", 'a') as outfile:
        subprocess.call(["BEVENT", "-y", fname[0:4], "-f", "0-96", fname[0:4]+"*.evn"], stdout=outfile)

    #Remove unzipped junk
    filelistRemove = [ f for f in os.listdir(".") if f.endswith(".EVA") ]
    for f in filelistRemove:
        os.remove(f)
    filelistRemove = [ f for f in os.listdir(".") if f.endswith(".EVN") ]
    for f in filelistRemove:
        os.remove(f)
    filelistRemove = [ f for f in os.listdir(".") if f.endswith(".ROS") ]
    for f in filelistRemove:
        os.remove(f)
    filelistRemove = [ f for f in os.listdir(".") if f.startswith("TEAM") ]
    for f in filelistRemove:
        os.remove(f)
