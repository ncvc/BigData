import math
import datetime

import matplotlib.pyplot as plt

from ParseData.Database import DB
from Lib import loadData, getPointsOfInterest


def plot(generateX, xLabel='x', yLabel='Taxi Pickups', includeFunc=None):
	with DB() as db:
		POIs = getPointsOfInterest()
		numRows, numCols = int(math.sqrt(len(POIs))), int(math.sqrt(len(POIs))) + 1

		for hour in xrange(24):
			plt.figure()
			fignum = 1
			for POI in POIs:
				print 'POI', POI
				x, y = loadData(db, POI['LAT'], POI['LONG'], generateX, includeFunc=includeFunc(hour))

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

	if weather.windchilli != None:
		temp = weather.windchilli
	elif weather.heatindexi != None:
		temp = weather.heatindexi
	else:
		temp = weather.tempi
	return temp

def generateNumPickupsBefore(db, latitude, longitude, time):
	return db.getNumPickupsNearLocation(latitude, longitude, time - datetime.timedelta(hours=2), time - datetime.timedelta(hours=1))

def generateNumPickupsAfter(db, latitude, longitude, time):
	return db.getNumPickupsNearLocation(latitude, longitude, time + datetime.timedelta(hours=3), time + datetime.timedelta(hours=4))

def generateAfterEvent(db, latitude, longitude, time):
	return db.afterNumEvents(latitude, longitude, time, 1)


# Include functions
def includeWeekday(weekday):
	return lambda db, latitude, longitude, time: time.weekday() == weekday

def includeHour(hour):
	return lambda db, latitude, longitude, time: time.hour == hour

if __name__ == '__main__':
	# plot(generateWeekday, xLabel='Day of week')
	plot(generateWeather, xLabel='Temp', includeFunc=includeHour)
