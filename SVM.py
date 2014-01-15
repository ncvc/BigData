import csv
import datetime
import time
import os

from sklearn import svm, preprocessing
from sklearn.pipeline import Pipeline

from ParseData.Database import DB
from ParseData.Config import DATA_FOLDER


TEST_DATASET_INTIAL_FILENAME = os.path.join(DATA_FOLDER, 'test1.txt')
TEST_DATASET_FINAL_FILENAME = os.path.join(DATA_FOLDER, 'test2.txt')
POINTS_OF_INTEREST_FILENAME = os.path.join(DATA_FOLDER, 'interestpoints.csv')
OUTPUT_FILENAME = 'out.txt'

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

REMOVED_TIMES = getRemovedTimes()

def getPointsOfInterest():
	with open(POINTS_OF_INTEREST_FILENAME) as f:
		POIs = []
		for point in csv.DictReader(f):
			point['LAT'] = float(point['LAT'])
			point['LONG'] = float(point['LONG'])
			POIs.append(point)
	return POIs

# Convenience method for populating inputs
# Inputs are the following:
#   Day of the week
#   Current time
#   Current Weather (tempi, precipm, hum, wspdi, windchilli, heatindexi, conds, fog, rain, snow, hail, thunder, tornado)
#   Number of pickups between 2 and 1 hour before the currentTime
#   Number of pickups between 3 and 4 hours after the currentTime
# TODO:
#   - Mixture Model?
#   - Use windchill/heatindex when available
#   - Use conds instead of rain
#   - Use different models for each of fog, rain, snow, etc.
def generateInputVector(db, time):
	weekday = time.weekday()
	hour = time.hour
	weather = db.getWeather(time)
	numPickupsBefore = db.getNumPickupsNearLocation(POI['LAT'], POI['LONG'], time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))
	# numPickupsAfter = db.getNumPickupsNearLocation(POI['LAT'], POI['LONG'], time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))
	precip = weather.rain
	if precip == None:
		precip = 0.0
	inputVector = (weekday, hour, numPickupsBefore, weather.tempi, precip)
	if any(inp == None for inp in inputVector):
		print inputVector
	return inputVector

def isRemovedTime(time, latitude, longitude):
	return (time, latitude, longitude) in REMOVED_TIMES

# Returns an x, y tuple representing the input and output
#
# Outputs are the number of pickups between currentTime and 2 hours from currentTime
def loadData(db, latitude, longitude, startTime=START_TIME, endTime=END_TIME):
	inputs = []
	outputs = []
	currentTime = startTime
	while currentTime < endTime:
		if not isRemovedTime(currentTime, latitude, longitude):
			# Populate the inputs
			inputs.append(generateInputVector(db, currentTime))

			# Calculate the number of pickups for the current hour
			numPickups = db.getNumPickupsNearLocation(latitude, longitude, currentTime, currentTime + datetime.timedelta(hours=2))
			outputs.append(numPickups)

		currentTime += datetime.timedelta(hours=1)
	return inputs, outputs

def generateSVM(db, latitude, longitude):
	print 'Loading Data'
	x, y = loadData(db, latitude, longitude)

	print 'Scaling Data'
	scaler = preprocessing.StandardScaler().fit(x)
	clf = svm.SVR(kernel='rbf', C=1e3, gamma=0.1)

	svmPipeline = Pipeline([('scaler', scaler), ('svr', clf)])
	print 'Training SVR'
	start = time.clock()
	svmPipeline.fit(x, y)
	print 'Total Training time:', time.clock() - start
	return svmPipeline

def predict(db, svmPipeline, latitude, longitude):
	print 'Begin Prediction'

	print 'Loading Test Dataset'
	testDataset = loadTestDataset()
	inputVectors = {sample['id']: generateInputVector(db, sample['start']) for sample in testDataset if sample['lat'] == latitude and sample['long'] == longitude}
	print 'Predicting', inputVectors
	try:
		predictions = svmPipeline.predict(inputVectors.values())
	except ValueError:
		predictions = [0] * len(inputVectors)

	return zip(inputVectors.keys(), predictions)


if __name__ == '__main__':
	predictions = []
	start = time.clock()
	with DB() as db:
		for POI in getPointsOfInterest():
			print 'POI', POI
			svmPipeline = generateSVM(db, POI['LAT'], POI['LONG'])
			predictions.extend(predict(db, svmPipeline, POI['LAT'], POI['LONG']))

	print 'All predictions took %s seconds' % (time.clock() - start)
	print 'Writing output'
	idList = [False] * len(loadTestDataset())
	outputList = []
	for locID, prediction in predictions:
		outputList.append('%i %i' % (locID, prediction))
		idList[locID] = True
	outputList.sort()

	with open(OUTPUT_FILENAME, 'w') as f:
		f.write('\n'.join(outputList))

	if not all(idList):
		print 'ERROR: MISSING PREDICTIONS'
