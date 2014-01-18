import csv
import os
import json
import time

from Database import DB
from Config import DATA_FOLDER


PICKUPS_FILENAME = os.path.join(DATA_FOLDER, 'pickups_train.csv')
DROPOFFS_FILENAME = os.path.join(DATA_FOLDER, 'dropoffs.csv')
WEATHER_FILENAME = os.path.join(DATA_FOLDER, 'wunderground.json')
EVENTS_FILENAME = os.path.join(DATA_FOLDER, 'events.csv')


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


if __name__ == '__main__':
	start = time.clock()
	with DB() as db:
		# parseWeather(db)
		# parsePickups(db)
		# parseDropoffs(db)
		parseEvents(db)
	print 'Total time:', time.clock() - start
