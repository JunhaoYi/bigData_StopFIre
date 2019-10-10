# Read both data into lists, seperating head and content for each attribute
import csv

with open('ClimateData-Part1.csv') as csvClimateDataFile:
    csvReader = csv.reader(csvClimateDataFile)
    climateList = list(csvReader)
    climateHead = climateList[0]
    climateContent = climateList[1:]

with open('FireData-Part1.csv') as csvFireDataFile:
    csvReader = csv.reader(csvFireDataFile)
    fireList = list(csvReader)
    fireHead = fireList[0]
    fireContent = fireList[1:]

    # Create mongodb collection called cfdata
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.fit5148
cfdata = db.cfcollection

# Use embeded model to build a new database based on the file we read from these 2 csv files.
# Climate Data contains a time period from 2016-12-31 to 2018-1-1, while Fire Data contains a time period from 2017-3-6 to 2017-12-27
# So for each climate data, there could be 0 to several fire data on that day, so we take fire data as an attribute of climate object.
# This model ensures every original record is covered

climateEmbededCollection = []
for climate in climateContent:
    fireOnOneDay = []
    for fire in fireContent:
        if climate[1] == fire[6]:  # if the date matches
            fireObj = {}
            for i in range(8):
                fireObj.update({fireHead[i - 1].strip(): fire[i - 1]})
            # convert surface temperature and confidence into int for pymongo find/match operations
            fireObj["Surface Temperature (Celcius)"] = int(fireObj["Surface Temperature (Celcius)"])
            fireObj["Confidence"] = int(fireObj["Confidence"])
            fireOnOneDay.append(fireObj)
    climateObj = {}
    for i in range(9):
        climateObj.update({climateHead[i - 1].strip(): climate[i - 1]})
    climateObj.update({"fire": fireOnOneDay})
    climateEmbededCollection.append(climateObj)

# insert collection into MongoDB
# we drop the collection first to insure records could replace the last version when this method is runagain
cfdata.drop()
cfdata.insert_many(climateEmbededCollection)

# Task A2
# Using pymongo
import pprint as pp

results = cfdata.find({"Date": "2017-12-15"})
for r in results:
    pp.pprint(r)

# Task A2 Find climate data on 15th December 2017.

# Using python parallel proccessing
from multiprocessing import Pool


def s_hashA2(x, n):
    # Convert last 2 digits of date(days) into int, hash base on reminder of it divided by number of processers
    x = int(x[-2:])
    result = x % n
    return result


# hash partition climate data into a dictionary with hashed data as keys.
def h_partitionA2(data, n):
    dic = {}
    for x in data:
        h = s_hashA2(x["Date"], n)
        if (h in dic.keys()):
            s = dic[h]
            s.append(x)
            dic[h] = s
        else:
            s = list(x)
            dic[h] = []
            dic[h].append(s)
    return dic


def linear_searchA2(data, key):
    matched_record = []
    for x in data:
        if x["Date"] == key:
            matched_record.append(x)
            break
    return matched_record


def parallel_searchA2(data, date, n_processor):
    results = []
    pool = Pool(processes=n_processor)

    DD = h_partitionA2(data, n_processor)
    date_hash = s_hashA2(date, n_processor)
    # Leave the first element of list as they are only names of attributes
    d = list(DD[date_hash])[1:]
    result = pool.apply(linear_searchA2, [d, date])
    results.extend(result)

    return results


# Task A3 Find the latitude longitude and confidence when the surface temperature (°C) was between 65 °C and 100 °C

import pprint as pp

results = cfdata.aggregate([{"$unwind": "$fire"},
                            {"$match": {"fire.Surface Temperature (Celcius)": {"$gte": 65, "$lte": 100}}},
                            {"$project": {"fire.Latitude": 1, "fire.Longitude": 1, "fire.Confidence": 1, "_id": 0}}])

for r in results:
    pp.pprint(r)

# Task A4 Find surface temperature (°C), air temperature (°C), relative humidity and maximum wind speed on 15th and 16th of December 2017

import pprint as pp

results = cfdata.find({"Date": {"$in": ["2017-12-15", "2017-12-16"]}},
                      {"fire.Surface Temperature (Celcius)": 1, "Air Temperature(Celcius)": 1, "Relative Humidity": 1,
                       "Max Wind Speed": 1, "_id": 0})
for r in results:
    pp.pprint(r)

# Task A5 Find datetime, air temperature (°C), surface temperature (°C) and confidence when the confidence is between 80 and 100

import pprint as pp

results = cfdata.aggregate([{"$unwind": "$fire"},
                            {"$match": {"fire.Confidence": {"$gte": 80, "$lte": 100}}},
                            {"$project": {"fire.Datetime": 1, "Air Temperature(Celcius)": 1,
                                          "fire.Surface Temperature (Celcius)": 1, "fire.Confidence": 1, "_id": 0}}])
for r in results:
    pp.pprint(r)

    # Task A6 Find top 10 records with highest surface temperature(°C)

import pprint as pp

results = cfdata.aggregate([{"$unwind": "$fire"},
                            {"$sort": {"fire.Surface Temperature (Celcius)": -1}},
                            {"$limit": 10},
                            {"$project": {"fire": 1}}])

for r in results:
    pp.pprint(r)

    # Task A7 Find the number of fire in each day.

    import pprint as pp

    results = cfdata.aggregate([{"$project": {"_id": 0, "Date": 1, "CountFire": {"$size": "$fire"}}}])
    for r in results:
        pp.pprint(r)

# Task A8 Find the average surface temperature(°C) for each day.

import pprint as pp

results = cfdata.aggregate([{"$project": {
    "Average Surface Temperature (Clecius)": {"$avg": "$fire.Surface Temperature (Celcius)"}, "_id": 0, "Date": 1}}])
for r in results:
    pp.pprint(r)
