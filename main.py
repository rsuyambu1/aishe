import os
import requests
import numpy as np
from datetime import datetime, timedelta
import csv
from dotenv import load_dotenv
import datetime as dt
from lib.db import Database
import xlrd
from fastapi import FastAPI, Query
from typing import Union

def download(url: str, dest_folder: str, filename:str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = filename  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 10000):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
                    return True
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))
        return False

def minmax1 (x):
    # this function fails if the list length is 0
    minimum = maximum = x[0]
    minIndex = maxIndex = 0
    for index, i in enumerate(x):
        if i < minimum:
            minimum = i
            minIndex = index
        else:
            if i > maximum:
                maximum = i
                maxIndex = index
    return (minimum,maximum, minIndex, maxIndex)

# Function to count occurrences
def countOccurrences(arr, x):
    count = 0
    n = len(arr)
    for i in range(n):
        if (arr[i] == x):
            count += 1
    return count

def csvwrite(writer, results):
    writer.writerow(results)

def getCurrentValFromCloud(destdir, filename, productInfo):
    url = os.getenv('CLOUD_FILE_PATH')+'actual.csv'
    download_status = download(url, destdir, filename)
    if(download_status):
        filename = destdir+filename
        with open(filename, 'r') as file:
            lastRow = file.readlines()[-1].split(";")
            lastRow[1] = lastRow[1].replace(",", ".")
            # lastRow[1] = xlrd.xldate_as_datetime(float(lastRow[1]), 0).strftime('%Y-%m-%d %H:%M:%S')
            lastRow[1] = xlrd.xldate_as_datetime(float(lastRow[1]), 0).strftime('%d.%m.%Y %H:%M:%S')
            currentVal = {'datetime': lastRow[1], 'time': lastRow[1].split(' ')[1]}
            for info in productInfo:
                index = productInfo[info][0]
                currentVal[info] = lastRow[index+4] + ',' + lastRow[index+5]
            return {'download_status': download_status, 'currentVal': currentVal}
    else:
        return {'download_status': False}

def matching(db, tablename, productInfo, matchingPositionLength, product, currentVal, duration):
    time = []
    low = []
    bid = []
    ask = []
    high = []
    valueLR = []
    currentTime = currentVal['time']
    dbconnection = db.get_connection()
    time_change = dt.timedelta(minutes=duration)
    date_time_obj = dt.datetime.strptime(currentVal['datetime'], '%d.%m.%Y %H:%M:%S')
    new_time = date_time_obj + time_change
    max_time = new_time.strftime("%d.%m.%Y %H:%M:%S")
    sql = "SELECT id, to_char(time, 'dd.mm.yyyy HH24:MI:SS') as currenttime, low, bid, ask, high, valuel, valuer, result  FROM " + tablename + " WHERE CAST(time AS time) > TIME '" + currentTime + "'";
    print(sql)
    dbconnection._cursor.execute(sql)
    results = dbconnection._cursor.fetchall()
    for result in results:
        time.append(result[1].replace(result[1][0:10], currentVal['datetime'][0:10]))
        low.append(result[2].strip())
        value = result[3]
        bid.append(value)
        value = result[4]
        ask.append(value)
        high.append(result[5].strip())
        valueLR.append(result[6].strip() + ',' + result[7].strip())
    arr = np.array(valueLR)
    currentVal = currentVal[product]
    print(currentVal, product, datetime.now())
    x = np.where(arr == currentVal)
    datalength = len(time)
    result = []
    if(len(x[0])):
        for StartMatchPoint in x[0]:
            tradingValue = '';
            endMatchPoint = StartMatchPoint + matchingPositionLength
            if(datalength < endMatchPoint):
                matchingPositionLength = datalength - StartMatchPoint
            bidCompare = np.full(matchingPositionLength, bid[x[0][0]])
            askCompare = np.full(matchingPositionLength, ask[x[0][0]])
            bidMatch = np.split(bid, [StartMatchPoint, endMatchPoint])
            bidMatch = bidMatch[1]
            askMatch = np.split(ask, [StartMatchPoint, endMatchPoint])
            askMatch = askMatch[1]
            bidResult1 = bidMatch < bidCompare
            askResult1 = askMatch < askCompare
            bidResult = minmax1(bidMatch)
            askResult = minmax1(askMatch)
            bidCount = countOccurrences(bidResult1, True)
            dataCount = round(len(bidResult1)/2)
            bidMinPosition = StartMatchPoint + bidResult[2]
            bidMaxPosition = StartMatchPoint + bidResult[3]
            askMinPosition = StartMatchPoint + askResult[2]
            askMaxPosition = StartMatchPoint + askResult[3]
            start = time[StartMatchPoint]
            if(dataCount <= bidCount):
                event = 'SELL'
                end = time[bidMinPosition]
                tradingValue = float(bidResult[1]) - float(bidResult[0])
            else:
                event = 'BUY'
                end = time[askMaxPosition]
                tradingValue = float(askResult[1]) - float(askResult[0])
            tradingValue = tradingValue * productInfo[1]
            tradingValue  = int(tradingValue)
            time_format = "%d.%m.%Y %H:%M:%S"
            dt1 = datetime.strptime(start, time_format)
            dt2 = datetime.strptime(end, time_format)
            print("time",  start, end)
            diff = ((dt2 - dt1) // timedelta(minutes=1))  # minutes
            if ((start <= max_time)):
                print('Product=',  product, ', CurrentValue=', currentVal, ',Start=', start, ', Duration=', diff, ', Event=', event, ', Value=', tradingValue)
                result.append([product, start, diff, event, tradingValue])
        return result
    else:
        print('No matching')
        return False

app = FastAPI()

@app.get("/")
async def read_items(q: Union[str, None] = Query(default=None, max_length=500)):
    if q:
        productInfo = {'EURUSD': [2, 100000, 5, 5], 'USDDKK': [9, 10000, 5, 5], 'USDCHF': [16, 100000, 5, 5], 'EURCAD': [23, 100000, 5, 5], 'USDCAD': [30, 100000, 5, 5], 'EURGBP': [37, 100000, 5, 5], 'GBPUSD': [44, 100000, 5, 5],
                     'AUDUSD': [51, 100000, 5,5], 'EURCHF': [58, 100000, 5, 5], 'AUDJPY': [65, 1, 3, 3], 'XAUUSD': [72, 100, 2, 2]}

        # Identify the date
        load_dotenv()
        curr_date = datetime.today()
        curr_day = curr_date.strftime('%A')
        week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        dayofweek = week.index(curr_day)
        week_day = dayofweek + 1
        sheetname = '.'.join([str(week_day), curr_day])
        filename = 'actual.csv'
        destdir = 'csv/'
        matchingPositionLength = 10
        resultfilename = sheetname + '_results.csv'
        resultfile = destdir + resultfilename;
        q = q.split('-')
        datatime = q[0].split("=")
        datatime = xlrd.xldate_as_datetime(float(datatime[1]), 0).strftime('%d.%m.%Y %H:%M:%S')
        currentVal = {'datetime': datatime, 'time': datatime.split(' ')[1]}
        download_status =True
        for info in q[1:]:
            pdata = info.split('=')
            currentVal[pdata[0]] = pdata[1]
        duration = int(os.getenv('DURATION'))
        db = Database()
        db.connect()
        if(download_status):
            filename = destdir+filename
            resultdata = {}
            resultStatus = True
            with open(resultfile, 'w', encoding='UTF8', newline='') as f:
                header = ['Product', 'Start', 'Duration', 'Event', 'Value']
                writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                for info in productInfo:
                    product = info
                    if(info in currentVal.keys()):
                        tablename = info + '_' + curr_day;
                        tablename = tablename.lower()
                        # db.settable(tablename)
                        results = matching(db, tablename, productInfo[product], matchingPositionLength, product, currentVal, duration)
                        if(results):
                            resultStatus = False
                            for result in results:
                                csvwrite(writer, result)

            if(resultStatus):
                with open(resultfile, 'w', encoding='UTF8', newline='') as f:
                    # message =
                    header = ['No matches found for next '+os.getenv('DURATION')+ ' Minutes']
                    writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(header)
        else:
            print("Download failed: status code")
        db.close()
        #os.remove(resultfilename)
    else:
        return False