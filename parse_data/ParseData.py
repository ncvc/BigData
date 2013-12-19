import csv
import os
import json

from Database import DB


DATA_FOLDER = 'C:\\Users\\nvcar_000\\Desktop\\Big Data Challenge'
PICKUPS_FILENAME = os.path.join(DATA_FOLDER, 'pickups_train.csv')
DROPOFFS_FILENAME = os.path.join(DATA_FOLDER, 'dropoffs.csv')
WEATHER_FILENAME = os.path.join(DATA_FOLDER, 'wunderground.json')


def parsePickups(db):
	print 'Parsing Taxi Pickups'
	with open(PICKUPS_FILENAME) as f:
		for row in csv.DictReader(f, fieldnames=['ID', 'DROPOFF_TIME', 'DROPOFF_ADDRESS', 'DROPOFF_LONG', 'DROPOFF_LAT']):
			db.addTaxiPickup(row)

def parseDropoffs(db):
	print 'Parsing Taxi Dropoffs'
	with open(DROPOFFS_FILENAME) as f:
		for row in csv.DictReader(f):
			db.addTaxiDropoff(row)

def parseWeather(db):
	print 'Parsing Weather'
	with open(WEATHER_FILENAME) as f:
		for response in json.load(f):
			for observation in response['history']['observations']:
				db.addWeather(observation)


if __name__ == '__main__':
	with DB() as db:
		parseWeather(db)
		parsePickups(db)
		parseDropoffs(db)
