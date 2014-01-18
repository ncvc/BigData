import csv
import os
import datetime

from ParseData.Config import DATA_FOLDER


TEST_DATASET_INTIAL_FILENAME = os.path.join(DATA_FOLDER, 'test1.txt')
TEST_DATASET_FINAL_FILENAME = os.path.join(DATA_FOLDER, 'test2.txt')
POINTS_OF_INTEREST_FILENAME = os.path.join(DATA_FOLDER, 'interestpoints.csv')

START_TIME = datetime.datetime(2012, 5, 1, 1)
# END_TIME = datetime.datetime(2012, 5, 3)
END_TIME = datetime.datetime(2012, 12, 1)


def loadTestDataset(filename=TEST_DATASET_INTIAL_FILENAME):
	# Read the data
	with open(filename) as f:
		testDataset = list(csv.DictReader(f, fieldnames=['id', 'start', 'end', 'lat', 'long']))
	# Convert to Python ints and datetimes
	for i in xrange(len(testDataset)):
		testDataset[i]['id'] = int(testDataset[i]['id'])
		testDataset[i]['start'] = datetime.datetime.strptime(testDataset[i]['start'], '%Y-%m-%d %H:%M')
		testDataset[i]['end'] = datetime.datetime.strptime(testDataset[i]['end'], '%Y-%m-%d %H:%M')
		testDataset[i]['lat'] = float(testDataset[i]['lat'])
		testDataset[i]['long'] = float(testDataset[i]['long'])
	return testDataset

def getRemovedTimes():
	initialTestDataset = loadTestDataset(filename=TEST_DATASET_INTIAL_FILENAME)
	finalTestDataset = loadTestDataset(filename=TEST_DATASET_FINAL_FILENAME)

	removedTimes = set()
	for dataset in (initialTestDataset, finalTestDataset):
		for time in dataset:
			currentTime = time['start'] - datetime.timedelta(hours=1)
			endTime = time['end'] + datetime.timedelta(hours=1)
			while currentTime < endTime:
				removedTimes.add((currentTime, time['lat'], time['long']))
				currentTime += datetime.timedelta(hours=1)

	return removedTimes

def getPointsOfInterest():
	with open(POINTS_OF_INTEREST_FILENAME) as f:
		POIs = []
		for point in csv.DictReader(f):
			point['LAT'] = float(point['LAT'])
			point['LONG'] = float(point['LONG'])
			POIs.append(point)
	return POIs

def isRemovedTime(time, latitude, longitude):
	return (time, latitude, longitude) in REMOVED_TIMES

# Returns an x, y tuple representing the input and output
#
# Outputs are the number of pickups between currentTime and 2 hours from currentTime
def loadData(db, latitude, longitude, generateX, startTime=START_TIME, endTime=END_TIME, includeFunc=None):
	inputs = []
	outputs = []
	currentTime = startTime
	if includeFunc == None:
		includeFunc = lambda db, latitude, longitude, currentTime: True
	while currentTime < endTime:
		if not isRemovedTime(currentTime, latitude, longitude) and includeFunc(db, latitude, longitude, currentTime):
			# Populate the inputs
			inputs.append(generateX(db, latitude, longitude, currentTime))

			# Calculate the number of pickups for the current hour
			numPickups = db.getNumPickupsNearLocation(latitude, longitude, currentTime, currentTime + datetime.timedelta(hours=2))
			outputs.append(numPickups)

		currentTime += datetime.timedelta(hours=1)
	return inputs, outputs

REMOVED_TIMES = getRemovedTimes()


# Convenience method for populating inputs
# Inputs are the following:
#   Day of the week
#   Current time
#   Current Weather (tempi, precipm, hum, wspdi, windchilli, heatindexi, conds, fog, rain, snow, hail, thunder, tornado)
#   Number of pickups between 2 and 1 hour before the currentTime
#   Number of pickups between 3 and 4 hours after the currentTime
# TODO:
#   - Mixture Model?
#   - Use conds instead of rain
#   - Use different models for each of fog, rain, snow, etc.
def generateAllFeatures(db, latitude, longitude, time):
	# Day of the week and time of day
	timeFeatures = [time.weekday(), time.hour]
	
	# Weather
	weather = db.getWeather(time)
	precip = weather.precipm
	if precip == None:
		precip = 0.0
	windchill = weather.windchilli
	if windchill == None:
		windchill = 0.0
	heatindex = weather.heatindexi
	if heatindex == None:
		heatindex = 0.0
	# Not weather.conds
	weatherFeatures = [weather.tempi, precip, weather.hum, weather.wspdi, windchill, heatindex, weather.fog, weather.rain, weather.snow, weather.thunder]

	# Events
	afterNumEvents = [db.afterNumEvents(latitude, longitude, time, i) for i in xrange(4)]
	duringNumEvents = [db.duringNumEvents(latitude, longitude, time - datetime.timedelta(hours=i)) for i in xrange(-4, 4)]

	eventFeatures = afterNumEvents + duringNumEvents

	# Pickups near the given timeframe
	numPickupsBefore = db.getNumPickupsNearLocation(latitude, longitude, time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))
	numPickupsAfter = db.getNumPickupsNearLocation(latitude, longitude, time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))
	pickupFeatures = [numPickupsBefore, numPickupsAfter]

	# Generate the vector
	inputVector = tuple(timeFeatures + weatherFeatures + pickupFeatures + eventFeatures)
	if any(inp == None for inp in inputVector):
		print 'Some input is None:', inputVector
	return inputVector


# Convenience method for populating inputs
# Inputs are the following:
#   Day of the week
#   Current time
#   Current Weather (tempi, precipm, hum, wspdi, windchilli, heatindexi, conds, fog, rain, snow, hail, thunder, tornado)
#   Number of pickups between 2 and 1 hour before the currentTime
#   Number of pickups between 3 and 4 hours after the currentTime
# TODO:
#   - Mixture Model?
#   - Use conds instead of rain
#   - Use different models for each of fog, rain, snow, etc.
def generateAllFeaturesExceptWeather(db, latitude, longitude, time):
	# Day of the week and time of day
	timeFeatures = [time.weekday(), time.hour]
	
	# Weather
	# weather = db.getWeather(time)
	# precip = weather.precipm
	# if precip == None:
	# 	precip = 0.0
	# windchill = weather.windchilli
	# if windchill == None:
	# 	windchill = 0.0
	# heatindex = weather.heatindexi
	# if heatindex == None:
	# 	heatindex = 0.0
	# # Not weather.conds
	# weatherFeatures = [weather.tempi, precip, weather.hum, weather.wspdi, windchill, heatindex, weather.fog, weather.rain, weather.snow, weather.thunder]

	# Events
	afterNumEvents = [db.afterNumEvents(latitude, longitude, time, i) for i in xrange(4)]
	duringNumEvents = [db.duringNumEvents(latitude, longitude, time - datetime.timedelta(hours=i)) for i in xrange(-4, 4)]

	eventFeatures = afterNumEvents + duringNumEvents

	# Pickups near the given timeframe
	numPickupsBefore = db.getNumPickupsNearLocation(latitude, longitude, time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))
	numPickupsAfter = db.getNumPickupsNearLocation(latitude, longitude, time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))
	pickupFeatures = [numPickupsBefore, numPickupsAfter]

	# Generate the vector
	inputVector = tuple(timeFeatures + pickupFeatures + eventFeatures)
	if any(inp == None for inp in inputVector):
		print 'Some input is None:', inputVector
	return inputVector
