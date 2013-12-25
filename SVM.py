import csv
import datetime
import time

from sklearn import svm, preprocessing
from sklearn.pipeline import Pipeline

from ParseData.Database import DB


TEST_DATASET_FILENAME = 'C:\\Users\\nvcar_000\\Desktop\\Big Data Challenge\\test1.txt'
POINTS_OF_INTEREST_FILENAME = 'C:\\Users\\nvcar_000\\Desktop\\Big Data Challenge\\interestpoints.csv'
OUTPUT_FILENAME = 'out.txt'

START_TIME = datetime.datetime(2012, 5, 1)
# END_TIME = datetime.datetime(2012, 5, 3)
END_TIME = datetime.datetime(2012, 12, 1)


def loadTestDataset(filename=TEST_DATASET_FILENAME):
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
#   Position
#   Current Weather (TODO)
#   Number of pickups between 2 and 1 hour before the currentTime
#   Number of pickups between 3 and 4 hours after the currentTime
def generateInputs(db, time, latitude, longitude):
	weekday = time.weekday()
	hour = time.hour
	# numPickupsBefore = db.getNumPickupsNearLocation(POI['LAT'], POI['LONG'], time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))
	# numPickupsAfter = db.getNumPickupsNearLocation(POI['LAT'], POI['LONG'], time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))
	return [weekday, hour, latitude, longitude]

# Returns an x, y tuple representing the input and output
#
# Outputs are the number of pickups between currentTime and 2 hours from currentTime
# TODO: Don't train on removed times
def loadData(startTime=START_TIME, endTime=END_TIME):
	inputs = []
	outputs = []
	POIs = getPointsOfInterest()
	currentTime = startTime
	with DB() as db:
		while currentTime < endTime:
			print float(currentTime.toordinal() - startTime.toordinal()) / (endTime.toordinal() - startTime.toordinal())
			for POI in POIs:
				# Populate the inputs
				inputs.append(generateInputs(db, currentTime, POI['LAT'], POI['LONG']))

				# Calculate the number of pickups for the current hour
				numPickups = db.getNumPickupsNearLocation(POI['LAT'], POI['LONG'], currentTime, currentTime + datetime.timedelta(hours=2))
				outputs.append(numPickups)
			currentTime += datetime.timedelta(hours=1)
	return inputs, outputs

def generateSVM():
	print 'Loading Data'
	x, y = loadData()

	print 'Scaling Data'
	scaler = preprocessing.StandardScaler().fit(x)
	clf = svm.SVR(kernel='rbf')

	svmPipeline = Pipeline([('scaler', scaler), ('svr', clf)])
	print 'Training SVR'
	time.clock()
	svmPipeline.fit(x, y)
	print 'Total Training time:', time.clock()
	return svmPipeline

def predict(svmPipeline, outputFilename=OUTPUT_FILENAME):
	print 'Begin Prediction'

	print 'Loading Test Dataset'
	testDataset = loadTestDataset()
	with DB() as db:
		inputs = [generateInputs(db, sample['start'], sample['lat'], sample['long']) for sample in testDataset]
	print 'Predicting'
	predictions = svmPipeline.predict(inputs)

	print 'Writing output'
	with open(outputFilename, 'w') as f:
		for sample, prediction in zip(testDataset, predictions):
			locId = sample['id']
			f.write('%i %i\n' % (locId, prediction))


if __name__ == '__main__':
	svmPipeline = generateSVM()
	predict(svmPipeline)
