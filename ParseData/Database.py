import datetime
import cPickle as pickle

from peewee import MySQLDatabase, Model, CharField, DateTimeField, IntegerField, BooleanField, TextField, DecimalField, fn

database = MySQLDatabase('big_data', host='localhost', port=3306, user='root', passwd='')

WEATHER_FIELD_LIST = ['heatindexm', 'windchillm', 'wdird', 'windchilli', 'hail', 'heatindexi', 'wgusti', 'thunder', 'pressurei', 'snow', 'pressurem', 'fog', 'vism', 'wgustm', 'tornado', 'hum', 'tempi', 'tempm', 'dewptm', 'rain', 'dewpti', 'precipm', 'wspdi', 'wspdm', 'visi']

PICKUPS_CACHE_FILENAME = 'query_cache.p'

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

class Event(BaseModel):
	event_id = IntegerField()
	name = TextField()
	start_time = DateTimeField()
	end_time = DateTimeField()
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)
	type = TextField()
	description = TextField(null=True)

	is_time_accurate = BooleanField()
	is_time_inferred = BooleanField()


# Decorator to cache database queries
def cached(func):
	def _cached(self, *args, **kwargs):
		if self.queryCache == None:
			self.loadQueryCache()

		key = (func.__name__, args, tuple(sorted(kwargs.items())))
		if key not in self.queryCache:
			result = func(self, *args, **kwargs)
			self.queryCache[key] = result
		return self.queryCache[key]

	return _cached

# Handles all database operations
class DB:
	def __enter__(self):
		database.connect()
		database.execute_sql('SET NAMES utf8mb4;')  # Necessary for some emojis
		self.queryCache = None
		return self

	def __exit__(self, excType, excValue, excTraceback):
		self.saveQueryCache()
		print 'DB.__exit__', excType, excValue, excTraceback
		database.close()

	def saveQueryCache(self):
		if self.queryCache != None:
			pickle.dump(self.queryCache, open(PICKUPS_CACHE_FILENAME, 'wb'))

	def loadQueryCache(self):
		try:
			self.queryCache = pickle.load(open(PICKUPS_CACHE_FILENAME, 'rb'))
		except IOError:
			self.queryCache = {}
		
	# Simple utility function to create tables if they do not exist
	def createTables(self):
		TaxiPickup.create_table(fail_silently=True)
		TaxiDropoff.create_table(fail_silently=True)
		Weather.create_table(fail_silently=True)
		Event.create_table(fail_silently=True)

	def isClose(self, lat, lon, obj, dist):
		return fn.pow(fn.pow(obj.latitude - lat, 2) + fn.pow(obj.longitude - lon, 2), 0.5) < dist

	# TODO: Use MySQL's Spatial Values to make this more efficient
	@cached
	def getNumPickupsNearLocation(self, lat, lon, startTime, endTime):
		return int(TaxiPickup.select().where(self.isClose(lat, lon, TaxiPickup, 0.00224946357) & TaxiPickup.time.between(startTime, endTime)).count())

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

	@cached
	def getWeather(self, time):
		halfAnHour = datetime.timedelta(hours=10)
		return Weather.select().where(Weather.time.between(time - halfAnHour, time + halfAnHour)).order_by(fn.ABS(fn.TIMEDIFF(Weather.time, time)).asc()).limit(1).get()

	def parseTimeStr(self, timeStrList):
		dateStr = timeStrList.pop(0)
		date = datetime.datetime.strptime(dateStr, '%m/%d/%Y').date()

		if len(timeStrList) == 0 or '/' in timeStrList[0]:
			return date, None

		timeStr = timeStrList.pop(0)

		hour, minute = [int(t) for t in timeStr.split(':')]

		isPM = int(timeStrList.pop(0))
		hour += 12 * isPM

		time = datetime.time(hour, minute)

		return date, time

	def metersToCoordDist(self, distInMeters):
		# 0.00224946357 = 250 meters
		return distInMeters / 250 * 0.00224946357

	@cached
	def afterNumEvents(self, lat, lon, time, maxHoursAfterEvent, distInMeters=250):
		dist = self.metersToCoordDist(distInMeters)
		earliestEndTime = time - datetime.timedelta(hours=maxHoursAfterEvent)
		return Event.select().where(self.isClose(lat, lon, Event, dist) & (Event.is_time_accurate == 1) & Event.end_time.between(earliestEndTime, time)).count()

	@cached
	def duringNumEvents(self, lat, lon, time, distInMeters=250):
		dist = self.metersToCoordDist(distInMeters)
		return Event.select().where(self.isClose(lat, lon, Event, dist) & (Event.is_time_accurate == 1) & (Event.start_time < time) & (Event.end_time > time)).count()

	# Infer the time
	def inferTime(self, description):
		return None

	def eventDictToSQLStrings(self, eventDict, fields):
		# Try to parse the (inconsistently formatted) time string
		timeStrList = [item for item in eventDict['time'].split(' ') if item != '']

		isAccurate = True
		isInferred = False
		for timeField in ('start_time', 'end_time'):
			date, time = self.parseTimeStr(timeStrList)

			# If no explicit time is given, try to infer it from the description
			if time == None:
				isAccurate = False
				time = self.inferTime(eventDict['description'])
				if time == None:
					time = datetime.time()
				else:
					isInferred = True

			eventDict[timeField] = datetime.datetime.combine(date, time).strftime('%Y-%m-%d %H:%M:%S')

		eventDict['is_time_accurate'] = isAccurate
		eventDict['is_time_inferred'] = isInferred

		return [eventDict[field] for field in fields]

	def addEvents(self, eventDicts):
		fields = ['event_id', 'name', 'address', 'longitude', 'latitude', 'type', 'description', 'start_time', 'end_time', 'is_time_accurate', 'is_time_inferred']
		# Paginate so the queries don't get too long
		index = 0
		insertsPerQuery = 1000
		types = set([eventType for eventDict in eventDicts for eventType in eventDict['type'].split(',')])
		with open('a.txt', 'w') as f:
			f.write('\n'.join(types))
		while index < len(eventDicts):
			print 'Percent complete:', 100.0 * index / len(eventDicts)

			args = [self.eventDictToSQLStrings(eventDict, fields) for eventDict in eventDicts[index:index+insertsPerQuery]]
			args = [sqlString for sqlStrings in args for sqlString in sqlStrings]

			toReplace = ','.join(['(%s)' % (','.join(['%s'] * len(fields)))] * (len(args) / len(fields)))
			
			database.execute_sql('INSERT INTO %s (%s) VALUES %s' % ('event', ','.join(fields), toReplace), args)

			index += insertsPerQuery

	def addEvent(self, eventDict):
		event = Event()

		event.event_id = eventDict['id']
		event.name = eventDict['name']
		event.address = eventDict['address']
		event.longitude = eventDict['long']
		event.latitude = eventDict['lat']
		event.type = eventDict['type']

		description = eventDict['description']
		if description == 'null':
			description = None
		event.description = description

		# Try to parse the (inconsistently formatted) time string
		timeStrList = [item for item in eventDict['time'].split(' ') if item != '']

		event.start_time = self.parseTimeStr(timeStrList)
		event.end_time = self.parseTimeStr(timeStrList)

		# event.save()


if __name__ == '__main__':
	with DB() as db:
		db.createTables()
