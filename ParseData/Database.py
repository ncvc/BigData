import datetime
import cPickle as pickle

from peewee import MySQLDatabase, Model, CharField, DateTimeField, IntegerField, BooleanField, TextField, DecimalField, fn

database = MySQLDatabase('big_data', host='localhost', port=3306, user='root', passwd='')

WEATHER_FIELD_LIST = ['heatindexm', 'windchillm', 'wdird', 'windchilli', 'hail', 'heatindexi', 'wgusti', 'thunder', 'pressurei', 'snow', 'pressurem', 'fog', 'vism', 'wgustm', 'tornado', 'hum', 'tempi', 'tempm', 'dewptm', 'rain', 'dewpti', 'precipm', 'wspdi', 'wspdm', 'visi']

PICKUPS_CACHE_FILENAME = 'pickups_cache.p'

# Database must use utf8mb4 for smileys and other such nonesense
# ALTER DATABASE hn CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


# Model definitions
class BaseModel(Model):
	class Meta:
		database = database

class TaxiPickup(BaseModel):
	trip_id = IntegerField()
	time = DateTimeField()
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)

class TaxiDropoff(BaseModel):
	trip_id = IntegerField()
	time = DateTimeField()
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)

# See weather API here: http://www.wunderground.com/weather/api/d/docs?d=resources/phrase-glossary
class Weather(BaseModel):
	tempm = DecimalField(max_digits=6, decimal_places=2, null=True)	      # Temp in C
	tempi = DecimalField(max_digits=6, decimal_places=2, null=True)       # Temp in F
	dewptm = DecimalField(max_digits=6, decimal_places=2, null=True)      # Dew point in C
	dewpti = DecimalField(max_digits=6, decimal_places=2, null=True)      # Dew point in F
	hum = DecimalField(max_digits=6, decimal_places=2, null=True)         # Humidity %
	wspdm = DecimalField(max_digits=6, decimal_places=2, null=True)       # Wind speed kph
	wspdi = DecimalField(max_digits=6, decimal_places=2, null=True)       # Wind speed in mph
	wgustm = DecimalField(max_digits=6, decimal_places=2, null=True)      # Wind gust in kph
	wgusti = DecimalField(max_digits=6, decimal_places=2, null=True)      # Wind gust in mph
	wdird = DecimalField(max_digits=6, decimal_places=2, null=True)       # Wind direction in degrees
	wdire = CharField(max_length=10, null=True)                           # Wind direction description (i.e., SW, NNE, Variable, etc.)
	vism = DecimalField(max_digits=6, decimal_places=2, null=True)        # Visibility in Km
	visi = DecimalField(max_digits=6, decimal_places=2, null=True)        # Visibility in Miles
	pressurem = DecimalField(max_digits=6, decimal_places=2, null=True)   # Pressure in mBar
	pressurei = DecimalField(max_digits=6, decimal_places=2, null=True)   # Pressure in inHg
	windchillm = DecimalField(max_digits=6, decimal_places=2, null=True)  # Wind chill in C
	windchilli = DecimalField(max_digits=6, decimal_places=2, null=True)  # Wind chill in F
	heatindexm = DecimalField(max_digits=6, decimal_places=2, null=True)  # Heat index C
	heatindexi = DecimalField(max_digits=6, decimal_places=2, null=True)  # Heat Index F
	precipm = DecimalField(max_digits=6, decimal_places=2, null=True)     # Precipitation in mm
	precipi = DecimalField(max_digits=6, decimal_places=2, null=True)     # Precipitation in inches
	conds = CharField(max_length=255, null=True)                          # Condition phrases, i.e. Light Drizzle, Heavy Drizzle, Drizzle, Overcast

	fog = BooleanField()
	rain = BooleanField()
	snow = BooleanField()
	hail = BooleanField()
	thunder = BooleanField()
	tornado = BooleanField()
	metar = CharField(max_length=255, null=True)

	icon = CharField(max_length=255, null=True)   # Short string indicating the current weather
	time = DateTimeField()


# Handles all database operations
class DB:
	def __enter__(self):
		database.connect()
		database.execute_sql('SET NAMES utf8mb4;')  # Necessary for some emojis
		self.pickupsCache = None
		return self

	def __exit__(self, type, value, traceback):
		self.savePickupsCache()
		print 'DB.__exit__', type, value, traceback
		database.close()

	def savePickupsCache(self):
		if self.pickupsCache != None:
			pickle.dump(self.pickupsCache, open(PICKUPS_CACHE_FILENAME, 'wb'))

	def loadPickupsCache(self):
		try:
			self.pickupsCache = pickle.load(open(PICKUPS_CACHE_FILENAME, 'rb'))
		except IOError:
			self.pickupsCache = {}
		
	# Simple utility function to create tables
	def createTables(self):
		TaxiPickup.create_table()
		TaxiDropoff.create_table()
		Weather.create_table()

	# TODO: Use MySQL's Spatial Values to make this more efficient
	def getNumPickupsNearLocation(self, lat, lon, startTime, endTime):
		if self.pickupsCache == None:
			self.loadPickupsCache()

		key = (lat, lon, startTime, endTime)
		if key not in self.pickupsCache:
			result = int(TaxiPickup.select().where((fn.pow(fn.pow(TaxiPickup.latitude - lat, 2) + fn.pow(TaxiPickup.longitude - lon, 2), 0.5) < 0.00224946357) & TaxiPickup.time.between(startTime, endTime)).count())
			self.pickupsCache[key] = result

		return self.pickupsCache[key]

	# Adds the taxi data to the db
	def addTaxiDropoffs(self, dropoffDicts):
		self.addTaxiDicts(dropoffDicts, 'taxidropoff', self.dropoffDictToSQLString)

	# Adds the taxi data to the db
	def addTaxiPickups(self, pickupDicts):
		self.addTaxiDicts(pickupDicts, 'taxipickup', self.pickupDictToSQLString)

	def dropoffDictToSQLString(self, dropoffDict):
		for format in ('%m/%d/%Y %H:%M', '%m/%d/%y %I:%M %p'):
			try:
				date = datetime.datetime.strptime(dropoffDict['DROPOFF_TIME'], format)
				break
			except ValueError:
				pass

		string = ','.join((dropoffDict['ID'], '"%s"' % date.strftime('%Y-%m-%d %H:%M:%S'), '"%s"' % dropoffDict['DROPOFF_ADDRESS'], dropoffDict['DROPOFF_LONG'], dropoffDict['DROPOFF_LAT']))
		return '(%s)' % string

	def pickupDictToSQLString(self, pickupDict):
		string = ','.join((pickupDict['ID'], '"%s"' % pickupDict['DROPOFF_TIME'], '"%s"' % pickupDict['DROPOFF_ADDRESS'], pickupDict['DROPOFF_LONG'], pickupDict['DROPOFF_LAT']))
		return '(%s)' % string

	def addTaxiDicts(self, taxiDicts, tableName, dictToSQLString):
		# Paginate so the queries don't get too long
		index = 0
		insertsPerQuery = 10000
		while index < len(taxiDicts):
			print 'Percent complete:', 100.0 * index / len(taxiDicts)

			args = ','.join((dictToSQLString(taxiDict) for taxiDict in taxiDicts[index:index+insertsPerQuery] if taxiDict['ID'] != 'ID' and taxiDict['ID'] != 'TRIP_ID'))
			database.execute_sql('INSERT INTO %s (trip_id, time, address, longitude, latitude) VALUES %s' % (tableName, args))

			index += insertsPerQuery

	# Adds the weather data to the db
	def addWeather(self, weatherDict):
		weather = Weather()

		# Check if the fields are invalid
		for field in WEATHER_FIELD_LIST:
			fieldValue = float(weatherDict[field])
			if fieldValue == -999 or fieldValue == -9999:
				fieldValue = None
			setattr(weather, field, fieldValue)

		weather.icon = weatherDict['icon']
		weather.conds = weatherDict['conds']
		weather.metar = weatherDict['metar']
		weather.wdire = weatherDict['wdire']

		timeDict = weatherDict['date']      # local time, use weatherDict['utcdate'] for UTC
		weather.time = datetime.datetime(int(timeDict['year']), int(timeDict['mon']), int(timeDict['mday']), int(timeDict['hour']), int(timeDict['min']))

		# Write the new row to the database
		weather.save()

	def getWeather(self, time):
		halfAnHour = datetime.timedelta(30 MINUTES!)
		return Weather.select().where(Weather.time.between(time - halfAnHour, time + halfAnHour))


if __name__ == '__main__':
	with DB() as db:
		print db.getPickupsNearLocation(42.343365, -71.057114)
		# count = 0
		# for x in db.getPickupsNearLocation(42.343365, -71.057114):
		# 	count += 1
		# 	print x.trip_id
		# print 'count', count
