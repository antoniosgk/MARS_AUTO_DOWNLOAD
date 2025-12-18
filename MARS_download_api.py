'''INFO ABOUT THE SCRIPT
INFO ABOUT THE SCRIPT:
########################################################################
This script is used in order to download data
from ECMWF MARS with web api:

https://confluence.ecmwf.int/display/WEBAPI/Web+API+FAQ

This version has been developed for downloading
MARS data from forecasts and from surface levels 
which are representative for the entire atmospheric column

In order to find the experiments visit the following link:

https://apps.ecmwf.int/mars-catalogue/?class=rd 

Then go to (it is just an example):

https://apps.ecmwf.int/mars-catalogue/?class=rd&expver=hprj 

Select atmospheric model:

https://apps.ecmwf.int/mars-catalogue/?stream=oper&class=rd&expver=hprj

Select either forecast or analysis links!!!

Here we focus on the forecasts!!!

You can select among the surface and the pressure (or the model) levels!!!

Here we focus on the surface which are available at:
https://apps.ecmwf.int/mars-catalogue/?levtype=sfc&type=fc&class=rd&stream=oper&expver=hprj

Select one day (***It is just an example since the script will work for requesting one or multiple days!!!***) 

https://apps.ecmwf.int/mars-catalogue/?stream=oper&levtype=sfc&expver=hprj&date=2021-09-01&type=fc&class=rd

Select your preferences from the step, time and parameter fields!!!

At the links below you can specify your request and you will find 
all the information needed to run this script (see below)

Here you can monitor your submissions:

https://apps.ecmwf.int/webmars/joblist/ 


#Limitations:
20 queued requests per user
2 active requests per user
'''
#%%
from ecmwfapi import *
from ecmwfapi import ECMWFDataServer,ECMWFService
import pandas as pd
import datetime as dt
import os
import logging
import sys
import shutil
#%%
#When the script starts
scriptstart=dt.datetime.now()
print ('Script starts at:',scriptstart)
#%%
#The url,key,mail are given by the user in order to have access in the data.In the same location
# a .ecmwfapirc should exist with the credentials.
url="https://api.ecmwf.int/v1"
key="fe1dcb573a3baa56c7ac659aa5f35508"
email="zerefos@geol.uoa.gr"
server = ECMWFDataServer(url=url,key=key,email=email)
print("✅ Connected to ECMWF API as", email)
server=ECMWFService("mars")
#%%
path_folder = r'C:\Users\antonis\OneDrive\Documents'   #change accordingly.In which folder data will be stored
regname = "25_OCTv4_EUROPE"  # name of the region
expver = "icki"   #name of the experiment
type_ = "fc"      #forecast or reanalysis
levtype = "sfc"    #leveltype surface or sth else

base_dir = os.path.join(path_folder, regname, expver, type_.upper(), levtype.upper()) #folder based on theprevious is created

if not os.path.exists(base_dir):                            #folder will be either created
    os.makedirs(base_dir)
    print("Folder created:", base_dir)
else:
    print("Folder already exists:", base_dir)                #or the code will confirm if it exists already
print('The data will be stored in the following folder: ',base_dir)    
#%%
# --- Logger setup ---
#here we set a logger setup where all info will be stored in a txt file named CAMS_log_txt
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("CAMS_log.txt", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
#%%
def get_last_downloaded_date(base_folder):
    """
    Finds the most recent date folder that contains at least one non-empty file.
    Handles one extra subfolder level (e.g., 00_00_00, 12_00_00)#it will be defined later in "mars parameter section".
    """
    valid_dates = []

    for item in os.listdir(base_folder):
        if item.isdigit() and len(item) == 8:
            date_folder = os.path.join(base_folder, item)
            if not os.path.isdir(date_folder):
                continue

            # Look for subfolders like 00_00_00 or 12_00_00
            subfolders = [
                sf for sf in os.listdir(date_folder)
                if os.path.isdir(os.path.join(date_folder, sf))
            ]

            found_nonempty = False

            # If there are subfolders, check inside them
            if subfolders:
                for sf in subfolders:
                    subfolder_path = os.path.join(date_folder, sf)
                    non_empty_files = [
                        f for f in os.listdir(subfolder_path)
                        if os.path.isfile(os.path.join(subfolder_path, f))
                        and os.path.getsize(os.path.join(subfolder_path, f)) > 0
                    ]
                    if non_empty_files:
                        found_nonempty = True
                        break
            else:
                # Fallback: check directly inside the date folder
                non_empty_files = [
                    f for f in os.listdir(date_folder)
                    if os.path.isfile(os.path.join(date_folder, f))
                    and os.path.getsize(os.path.join(date_folder, f)) > 0
                ]
                if non_empty_files:
                    found_nonempty = True

            if found_nonempty:
                valid_dates.append(item)
            else:
                logger.warning(f" Folder {item} exists but is empty or its subfolders contain only 0-byte files.")

    if not valid_dates:
        logger.info("No valid (non-empty) data folders found.")
        return None

    latest_date = max(valid_dates)
    logger.info(f"Last non-empty data folder found: {latest_date}")
    return latest_date
today = dt.datetime.utcnow()
last_date = get_last_downloaded_date(base_dir) #the previous function is called


if last_date:
    start_date = dt.datetime.strptime(last_date, "%Y%m%d") + dt.timedelta(days=1)
    if start_date.date() > today.date():
        logger.info(f"Data are already up to date until {last_date}. No new downloads needed.")
        raise SystemExit()
    else:
        logger.info(f"Continuing download from {start_date.strftime('%Y%m%d')}.") #we try to download data from the current day(today) and before
else:
    start_date = today - dt.timedelta(days=5)     #IMPORTANT:days can change based on how many days in the past we want to go
                                               #if we want the data from the last week days=7
    logger.info(f"No previous valid data found. Starting from {start_date.strftime('%Y%m%d')}.")
#we get warnings that a folder exists but its subfolders may contain O byte files or that a folder does not exist
#It prints which is the last not empty data folder found
#%%
# MARS PARAMETERS ,The user should change the parameters
# ======================================================
classs = "rd"      #          see the documentation
stream = "oper"    #
grid = "0.4/0.4"   #
area = "30/0/28/2"   #Domain N/W/S/E
param = "207.210/209.210"  #
times = ["00:00:00"]  #   model initialization time
step = "0/12"    # eg 0/6/12/18
format_ = "netcdf"   # or grib
#%%
empty_folders = []  # store folders with no data
downloaded_folders = []  # store successful folders

date = start_date
today = dt.datetime.utcnow()

while date <= today:
    rundate = date.strftime("%Y%m%d")
    rundate_folder = os.path.join(base_dir, rundate)

    # --- Folder for each date ---
    if not os.path.exists(rundate_folder):
        os.makedirs(rundate_folder)
        logger.info(f" Created folder for {rundate}")

    day_has_data = False  # flag to track if any file was downloaded for this date

    for time in times:
        outfolder = os.path.join(rundate_folder, time.replace(":", "_"))
        if not os.path.exists(outfolder):
            os.makedirs(outfolder)
            logger.info(f" Created subfolder: {outfolder}")

        outfile = f"CAMS_{expver}_{type_}_{levtype}_{time.replace(':', '_')}_{rundate}.nc"
        outpath = os.path.join(outfolder, outfile)
         # Skip if already exists
        if os.path.exists(outpath):
            logger.info(f"File already exists: {outfile}")
            day_has_data = True
            continue

        # Try to download
        try:
            logger.info(f" Downloading {outfile} ...")
            request = {
                "class": classs,
                "type": type_,
                "stream": stream,
                "expver": expver,
                "levtype": levtype,
                "param": param,
                "date": rundate,
                "time": time,
                "step": step,
                "area": area,
                "grid": grid,
                "format": format_
            }
            server.execute(request, target=outpath)
            logger.info(f" Download completed: {outpath}")
            day_has_data = True

        except Exception as e:
            logger.error(f"Error downloading {outfile}: {e}")

    # After finishing all times for the day
    if day_has_data:
        downloaded_folders.append(rundate)
    else:
        empty_folders.append(rundate)
        logger.warning(f" Folder {rundate_folder} remains empty — likely data not uploaded yet.")

    date += dt.timedelta(days=1)
# ======================================================
# === FINAL SUMMARY MESSAGE ===
# ======================================================
if not empty_folders:
    logger.info("All data have been successfully downloaded for all available dates!")
else:
    logger.warning(f" Data for the following dates remain empty (not uploaded yet or failed): {', '.join(empty_folders)}")

logger.info("=== CAMS automatic data check finished ===")

print()
print(" Download summary:")
if not empty_folders:
    print("All data have been successfully downloaded for all dates.")
else:
    print(f"The following data dates folder remain empty: {', '.join(empty_folders)}")
scriptend=dt.datetime.now()
print ("")
print ("")
print ("")
print ("The script ended at:", scriptend)
print ("")
print ("")
print ("")
print ("The execution time was:", scriptend-scriptstart)
print ("")
print ("")
print ("")           
# %%
