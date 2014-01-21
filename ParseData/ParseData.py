import csv
import os
import json
import time
import cPickle as pickle
import datetime

from Database import DB
from Config import DATA_FOLDER


PICKUPS_FILENAME = os.path.join(DATA_FOLDER, 'pickups_train.csv')
DROPOFFS_FILENAME = os.path.join(DATA_FOLDER, 'dropoffs.csv')
WEATHER_FILENAME = os.path.join(DATA_FOLDER, 'wunderground.json')
EVENTS_FILENAME = os.path.join(DATA_FOLDER, 'events.csv')
TWEETS_FILENAMES = [os.path.join(DATA_FOLDER, '2012_%02i.json' % month) for month in xrange(4, 13)]
BUS_FILENAMES = [os.path.join(DATA_FOLDER, 'mbta', 'AFC_%s2012.rpt' % month) for month in ('May', 'June')]
T_FILENAMES = [os.path.join(DATA_FOLDER, 'mbta', 'ODRail_%s2012.rpt' % month) for month in ('May', 'June')]
STOPS_FILENAME = os.path.join(DATA_FOLDER, 'mbta', 'Stops.rpt')


def parsePickups(db):
	print 'Parsing Taxi Pickups'
	with open(PICKUPS_FILENAME) as f:
		db.addTaxiPickups(list(csv.DictReader(f, fieldnames=['ID', 'DROPOFF_TIME', 'DROPOFF_ADDRESS', 'DROPOFF_LONG', 'DROPOFF_LAT'])))

def parseDropoffs(db):
	print 'Parsing Taxi Dropoffs'
	with open(DROPOFFS_FILENAME) as f:
		db.addTaxiDropoffs(list(csv.DictReader(f)))

def parseWeather(db):
	print 'Parsing Weather'
	with open(WEATHER_FILENAME) as f:
		for response in json.load(f):
			for observation in response['history']['observations']:
				db.addWeather(observation)

def parseEvents(db):
	print 'Parsing Events'
	with open(EVENTS_FILENAME) as f:
		events = []
		for row in csv.DictReader(f, fieldnames=['event_id', 'name', 'time', 'address', 'type', 'description', 'extra1', 'extra2'], quoting=csv.QUOTE_NONE, delimiter='\t'):
			if row['extra1'] != None:
				row['latitude'] = row['type']
				row['longitude'] = row['description']
				row['type'] = row['extra1']
				row['description'] = row['extra2']
				events.append(row)
				break
			addressList = row['address'].split(' ')
			row['latitude'] = addressList[-2]
			row['longitude'] = addressList[-1]
			row['address'] = ' '.join(addressList[:-2])
			events.append(row)

		for row in csv.DictReader(f, fieldnames=['event_id', 'name', 'time', 'address', 'latitude', 'longitude', 'type', 'description'], quoting=csv.QUOTE_NONE, delimiter='\t'):
			events.append(row)

		db.addEvents(events)

def parseTweets(db):
	print 'Parsing Tweets'
	for tweetFilename in TWEETS_FILENAMES:
		print 'Parsing %s' % tweetFilename
		with open(tweetFilename) as f:
			tweets = (json.loads(line.strip()) for line in f if line != '\n')
			db.addTweets(tweets)

def parseLine(fieldIndices, line):
	parsedLine = []
	lastIndex = 0
	for fieldIndex in fieldIndices[1:] + [len(line)]:
		parsedLine.append(line[lastIndex:fieldIndex].strip())
		lastIndex = fieldIndex

	return parsedLine

def parseTRides(db):
	print 'Parsing T Rides'
	for TFilename in T_FILENAMES:
		print 'Parsing %s' % TFilename
		with open(TFilename) as f:
			header = f.readline()[3:]
			separator = f.readline()

			# Find the length of each field by parsing the separator line
			fieldIndices = []
			index = 0
			while index != -1:
				fieldIndices.append(index+1)
				index = separator.find(' ', index+1)

			fieldNames = parseLine(fieldIndices, header)

			TRides = (dict(zip(fieldNames, parseLine(fieldIndices, line))) for line in f)
			db.addTRides(TRides)

def parseTRides2(db):
	print 'Parsing T Rides'
	places = {}
	for TFilename in T_FILENAMES:
		print 'Parsing %s' % TFilename
		with open(TFilename) as f:
			header = f.readline()[3:]
			separator = f.readline()

			# Find the length of each field by parsing the separator line
			fieldIndices = []
			index = 0
			while index != -1:
				fieldIndices.append(index+1)
				index = separator.find(' ', index+1)

			fieldNames = parseLine(fieldIndices, header)
			for line in f:
				if line == '' or line =='\n':
					break

				SQLDict = db.TDictToSQLStrings(dict(zip(fieldNames, parseLine(fieldIndices, line))))
				for label in 'origin', 'destination':
					place = SQLDict[label]
					if place not in places:
						places[place] = {}
					try:
						timeObj = datetime.datetime.strptime(SQLDict['datetime'][:13], '%Y-%m-%d %H')
					except ValueError:
						print line
						raise
					if timeObj not in places[place]:
						places[place][timeObj] = {}
						places[place][timeObj]['origin'] = 0
						places[place][timeObj]['destination'] = 0

					places[place][timeObj][label] += 1

	with open('places.p', 'wb') as f:
		pickle.dump(places, f)


def parseBusRides(db):
	print 'Parsing Bus Rides'
	for busFilename in BUS_FILENAMES:
		print 'Parsing %s' % busFilename
		with open(busFilename) as f:
			header = f.readline()[3:]
			separator = f.readline()

			# Find the length of each field by parsing the separator line
			fieldIndices = []
			index = 0
			while index != -1:
				fieldIndices.append(index+1)
				index = separator.find(' ', index+1)

			fieldNames = parseLine(fieldIndices, header)

			busRides = (dict(zip(fieldNames, parseLine(fieldIndices, line))) for line in f)
			db.addBusRides(busRides)

def parseStops(db):
	print 'Parsing Stops'
	with open(STOPS_FILENAME) as f:
		header = f.readline()[3:]
		separator = f.readline()

		# Find the length of each field by parsing the separator line
		fieldIndices = []
		index = 0
		while index != -1:
			fieldIndices.append(index+1)
			index = separator.find(' ', index+1)

		fieldNames = parseLine(fieldIndices, header)

		stops = (dict(zip(fieldNames, parseLine(fieldIndices, line))) for line in f)
		db.addStops(stops)

if __name__ == '__main__':
	start = time.clock()

	with DB() as db:
		parseTRides2(db)

	print 'Total time:', time.clock() - start
