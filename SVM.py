import time

from sklearn import svm, preprocessing, gaussian_process, neighbors, cross_decomposition, ensemble
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectPercentile, f_regression, SelectKBest

from ParseData.Database import DB
from Lib import loadData, loadTestDataset, getPointsOfInterest, generateAllFeatures


OUTPUT_FILENAME = 'out.txt'


# Various machine learning methods
def generateSVMPipeline(x):
	selector = SelectKBest(f_regression, k=5)
	scaler = preprocessing.StandardScaler().fit(x)
	reg = svm.SVR(kernel='rbf', C=1e3, gamma=0.1)

	return Pipeline([('selector', selector), ('scaler', scaler), ('svr', reg)])

def generateGaussianProcessPipeline(x):
	selector = SelectKBest(f_regression, k=5)
	scaler = preprocessing.StandardScaler().fit(x)
	gp = gaussian_process.GaussianProcess()

	return Pipeline([('selector', selector), ('scaler', scaler), ('gp', gp)])

def generateKNearestNeighborsPipeline(x):
	selector = SelectKBest(f_regression, k=5)
	scaler = preprocessing.StandardScaler().fit(x)
	knn = neighbors.KNeighborsRegressor(n_neighbors=5, weights='uniform')

	return Pipeline([('selector', selector), ('scaler', scaler), ('knn', knn)])

def generateRadiusNearestNeighborsPipeline(x):
	selector = SelectKBest(f_regression, k=5)
	scaler = preprocessing.StandardScaler().fit(x)
	rnn = neighbors.RadiusNeighborsRegressor(weights='uniform')

	return Pipeline([('selector', selector), ('scaler', scaler), ('rnn', rnn)])

def generatePLSRegressionPipeline(x):
	scaler = preprocessing.StandardScaler().fit(x)
	pls = cross_decomposition.PLSRegression(n_components=2)

	return Pipeline([('scaler', scaler), ('pls', pls)])

def generateRandomForestPipeline(x):
	scaler = preprocessing.StandardScaler().fit(x)
	rf = ensemble.RandomForestRegressor(n_estimators=10)

	return Pipeline([('scaler', scaler), ('rf', rf)])

def generateExtraTreesPipeline(x):
	scaler = preprocessing.StandardScaler().fit(x)
	et = ensemble.ExtraTreesRegressor(n_estimators=10)

	return Pipeline([('scaler', scaler), ('et', et)])

def generateAdaBoostPipeline(x):
	scaler = preprocessing.StandardScaler().fit(x)
	base_estimator = ensemble.DecisionTreeRegressor(max_depth=4)
	ada = ensemble.AdaBoostRegressor(base_estimator=base_estimator, n_estimators=300)

	return Pipeline([('scaler', scaler), ('ada', ada)])

def generateGradientBoostingPipeline(x):
	scaler = preprocessing.StandardScaler().fit(x)
	grb = ensemble.GradientBoostingRegressor()

	return Pipeline([('scaler', scaler), ('grb', grb)])	


# Fitting and predicting
def fitPipeline(db, latitude, longitude, generatePipeline):
	print 'Loading Data'
	x, y = loadData(db, latitude, longitude, generateAllFeatures)

	print 'Generating pipeline'
	pipeline = generatePipeline(x)

	print 'Training SVR'
	start = time.clock()
	pipeline.fit(x, y)
	print 'Total Training time:', time.clock() - start
	return pipeline

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
	generatePipeline = generateSVMPipeline

	predictions = []
	start = time.clock()
	numNegatives = 0
	with DB() as db:
		for POI in getPointsOfInterest():
			print 'POI', POI
			fitPipeline = fitPipeline(db, POI['LAT'], POI['LONG'], generatePipeline)
			POIPredictions, POINegatives = predict(db, fitPipeline, POI['LAT'], POI['LONG'])
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
