import math
import datetime

import matplotlib.pyplot as plt
from sklearn.feature_selection import SelectPercentile, f_regression
from sklearn.svm import SVR
from sklearn.cross_validation import StratifiedKFold
from sklearn.feature_selection import RFECV
import numpy as np

from ParseData.Database import DB
from Lib import loadData, getPointsOfInterest, generateAllFeatures, generateAllFeaturesExceptWeather


def plot(generateX, xLabel='x', yLabel='Taxi Pickups', includeFunc=None):
	with DB() as db:
		POIs = getPointsOfInterest()
		numRows, numCols = int(math.sqrt(len(POIs))), int(math.sqrt(len(POIs))) + 1

		# for hour in xrange(24):
		plt.figure()
		plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.5, hspace=0.5)
		fignum = 1
		for POI in POIs:
			print 'POI', POI
			x, y = loadData(db, POI['LAT'], POI['LONG'], generateX, includeFunc=includeFunc)

			plt.subplot(numRows, numCols, fignum)
			plt.scatter(x, y)
			plt.title(POI['NAME'])
			plt.xlabel(xLabel)
			plt.ylabel(yLabel)

			fignum += 1
	plt.show()


# Generate functions
def generateWeekday(db, latitude, longitude, time):
	return time.weekday()

def generateHour(db, latitude, longitude, time):
	return time.hour

def generateWeather(db, latitude, longitude, time):
	weather = db.getWeather(time)
	return weather.tornado

def generateNumPickupsBefore(db, latitude, longitude, time):
	return db.getNumPickupsNearLocation(latitude, longitude, time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))

def generateNumPickupsAfter(db, latitude, longitude, time):
	return db.getNumPickupsNearLocation(latitude, longitude, time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))

def generateAfterEvent(db, latitude, longitude, time):
	return db.afterNumEvents(latitude, longitude, time, 3)

def generateDuringEvent(db, latitude, longitude, time):
	# timedelta = datetime.timedelta(hours=1)
	return db.isDuringEvent(latitude, longitude, time)

# Include functions
def includeWeekday(weekday):
	return lambda db, latitude, longitude, time: time.weekday() == weekday

def includeHour(hour):
	return lambda db, latitude, longitude, time: time.hour == hour

# Feature Selection

def featureSelection():
	with DB() as db:
		POIs = getPointsOfInterest()
		numRows, numCols = int(math.sqrt(len(POIs))), int(math.sqrt(len(POIs))) + 1

		# for hour in xrange(24):
		plt.figure()
		plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.5, hspace=0.5)
		fignum = 1
		for POI in POIs:
			x, y = loadData(db, POI['LAT'], POI['LONG'], generateAllFeaturesExceptWeather)
			x, y = np.array(x), np.array(y)

			###############################################################################
			width = 0.6

			x_indices = np.arange(x.shape[-1])

			###############################################################################
			# Univariate feature selection with F-test for feature scoring
			# We use the default selection function: the 10% most significant features
			selector = SelectPercentile(f_regression, percentile=10)
			selector.fit(x, y)
			scores = -np.log10(selector.pvalues_)
			# scores /= scores.max()

			plt.subplot(numRows, numCols, fignum)

			plt.bar(x_indices-(width/2), scores, width=width, color='g')
			plt.title(POI['NAME'])
			plt.xlabel('Feature number')
			plt.ylabel('Univariate score ($-Log(p_{value})$)')
			plt.xticks(x_indices)
			plt.axis('tight')
			plt.legend(loc='upper right')

			fignum += 1
	plt.show()

def recursiveFeatureElimination():
	with DB() as db:
		POIs = getPointsOfInterest()
		numRows, numCols = int(math.sqrt(len(POIs))), int(math.sqrt(len(POIs))) + 1

		# for hour in xrange(24):
		plt.figure()
		plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.5, hspace=0.5)
		fignum = 1
		for POI in POIs:
			x, y = loadData(db, POI['LAT'], POI['LONG'], generateAllFeatures)
			x, y = np.array(x), np.array(y)

			# Create the RFE object and compute a cross-validated score.
			svr = SVR(kernel="linear")
			rfecv = RFECV(estimator=svr, step=1, cv=StratifiedKFold(y, 2), scoring='accuracy')
			rfecv.fit(x, y)

			print("Optimal number of features : %d" % rfecv.n_features_)

			# Plot number of features VS. cross-validation scores
			plt.subplot(numRows, numCols, fignum)
			plt.title(POI['NAME'])
			plt.xlabel("Number of features selected")
			plt.ylabel("Cross validation score (nb of misclassifications)")
			plt.plot(range(1, len(rfecv.grid_scores_) + 1), rfecv.grid_scores_)

			fignum += 1
	plt.show()

if __name__ == '__main__':
	# plot(generateWeekday, xLabel='Day of week')
	# plot(generateAfterEvent, xLabel='Num of ongoing events')
	featureSelection()
