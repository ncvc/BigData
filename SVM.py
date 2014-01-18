import datetime
import time

from sklearn import svm, preprocessing
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectPercentile, f_regression, SelectKBest

from ParseData.Database import DB
from Lib import loadData, loadTestDataset, getPointsOfInterest, generateAllFeatures


OUTPUT_FILENAME = 'out.txt'


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
def generateInputVector(db, latitude, longitude, time):
	# Day of the week and time of day
	weekday = time.weekday()
	hour = time.hour
	
	# Weather
	weather = db.getWeather(time)
	precip = weather.rain
	if precip == None:
		precip = 0.0
	# if weather.windchilli != None:
	# 	temp = weather.windchilli
	# elif weather.heatindexi != None:
	# 	temp = weather.heatindexi
	# else:
	# temp = weather.tempi

	# Events
	# afterNumEvents = db.afterNumEvents(latitude, longitude, time, 1)

	# Pickups near the given timeframe
	numPickupsBefore = db.getNumPickupsNearLocation(latitude, longitude, time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))
	numPickupsAfter = db.getNumPickupsNearLocation(latitude, longitude, time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))

	# Generate the vector
	inputVector = (hour, numPickupsBefore, numPickupsAfter, weekday, weather.rain, weather.snow, weather.thunder)
	if any(inp == None for inp in inputVector):
		print 'Some input is None:', inputVector
	return inputVector

def generateSVM(db, latitude, longitude):
	print 'Loading Data'
	x, y = loadData(db, latitude, longitude, generateAllFeatures)

	print 'Scaling Data'
	selector = SelectKBest(f_regression, k=5)
	scaler = preprocessing.StandardScaler().fit(x)
	reg = svm.SVR(kernel='rbf', C=1e3, gamma=0.1)

	svmPipeline = Pipeline([('selector', selector), ('scaler', scaler), ('svr', reg)])
	print 'Training SVR'
	start = time.clock()
	svmPipeline.fit(x, y)
	print 'Total Training time:', time.clock() - start
	return svmPipeline

def predict(db, svmPipeline, latitude, longitude):
	print 'Begin Prediction'

	print 'Loading Test Dataset'
	testDataset = loadTestDataset()
	inputVectors = {sample['id']: generateAllFeatures(db, latitude, longitude, sample['start']) for sample in testDataset if sample['lat'] == latitude and sample['long'] == longitude}
	print 'Predicting', inputVectors
	try:
		predictions = svmPipeline.predict(inputVectors.values())
	except ValueError:
		predictions = [0] * len(inputVectors)

	# Ensure we don't predict any negative values
	finalPredictions = []
	numNegatives = 0
	for prediction in predictions:
		finalPrediction = 0
		if prediction > 0:
			finalPrediction = prediction
		elif prediction < 0:
			numNegatives += 1
		finalPredictions.append(finalPrediction)


	return zip(inputVectors.keys(), finalPredictions), numNegatives


if __name__ == '__main__':
	predictions = []
	start = time.clock()
	numNegatives = 0
	with DB() as db:
		for POI in getPointsOfInterest():
			print 'POI', POI
			svmPipeline = generateSVM(db, POI['LAT'], POI['LONG'])
			POIPredictions, POINegatives = predict(db, svmPipeline, POI['LAT'], POI['LONG'])
			predictions.extend(POIPredictions)
			numNegatives += POINegatives

	print 'Predicted a negative number of taxi pickups %i times' % numNegatives

	print 'All predictions took %s seconds' % (time.clock() - start)
	print 'Writing output'
	idList = [False] * len(loadTestDataset())
	outputList = []
	for locID, prediction in predictions:
		outputList.append((locID, '%i %i' % (locID, prediction)))
		idList[locID] = True
	outputList.sort()

	with open(OUTPUT_FILENAME, 'w') as f:
		f.write('\n'.join([out[1] for out in outputList]))

	if not all(idList):
		print 'ERROR: MISSING PREDICTIONS'
