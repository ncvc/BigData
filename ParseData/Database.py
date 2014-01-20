import datetime
import time
import cPickle as pickle
from HTMLParser import HTMLParser

from peewee import MySQLDatabase, Model, fn
from peewee import CharField, DateTimeField, DateField, IntegerField, BooleanField, TextField, DecimalField, BigIntegerField

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
	time = DateTimeField(index=True)
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)

class TaxiDropoff(BaseModel):
	trip_id = IntegerField()
	time = DateTimeField(index=True)
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
	time = DateTimeField(index=True)

class Event(BaseModel):
	event_id = IntegerField()
	name = TextField()
	start_time = DateTimeField(index=True)
	end_time = DateTimeField(index=True)
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)
	type = TextField()
	description = TextField(null=True)

	is_time_accurate = BooleanField(index=True)
	is_time_inferred = BooleanField(index=True)

class Tweet(BaseModel):
	text = CharField(max_length=255, null=True)
	longitude = DecimalField(max_digits=9, decimal_places=6, null=True)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6, null=True)   # Range: (-90, 90)
	created_at = DateTimeField(formats='%Y-%m-%d %H:%M:%S', null=True, index=True)
	#TODO: entities
	favorited = BooleanField(null=True)
	tweet_id = BigIntegerField(null=True)
	place_id = CharField(max_length=255, null=True)
	retweet_count = IntegerField(null=True)
	source = CharField(max_length=255, null=True)
	user_id = BigIntegerField(null=True)

	mentions_taxi = BooleanField(index=True)

class TRide(BaseModel):
	ticket_type = CharField(max_length=255)
	origin = CharField(max_length=50, index=True)
	destination = CharField(max_length=50, index=True)
	trips = IntegerField()
	date = DateField(formats='%Y-%m-%d', index=True)
	datetime = DateTimeField(formats='%Y-%m-%d %H:%M:%S.000', index=True)
	next_trip_date = DateTimeField(formats='%Y-%m-%d %H:%M:%S.000', index=True)

class BusRide(BaseModel):
	device_id = IntegerField()
	ticket_type = CharField(max_length=255)
	device_class_id = IntegerField()
	date = DateField(formats='%Y-%m-%d', index=True)
	datetime = DateTimeField(formats='%Y-%m-%d %H:%M:%S.000', index=True)
	route_station = CharField(max_length=50, index=True)

class Stop(BaseModel):
	loc_id = IntegerField()
	is_station = BooleanField()
	direction = CharField(max_length=255)
	next_loc_id = IntegerField()
	notes = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)
	next_station = IntegerField()
	prev_station = IntegerField()
	station = CharField(max_length=50, index=True)
	sort_order = DecimalField(max_digits=4, decimal_places=2)   # Range: (-90, 90)
	line = CharField(max_length=10)
	circuit_number = CharField(max_length=30)
	heading = IntegerField()
	loc_type = CharField(max_length=30)


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
		Tweet.create_table(fail_silently=True)
		TRide.create_table(fail_silently=True)
		BusRide.create_table(fail_silently=True)
		Stop.create_table(fail_silently=True)

	def getDist(self, lat, lon, obj):
		return fn.pow(fn.pow(obj.latitude - lat, 2) + fn.pow(obj.longitude - lon, 2), 0.5)

	def isClose(self, lat, lon, obj, dist):
		return self.getDist(lat, lon, obj) < dist

	# TODO: Use MySQL's Spatial Values to make this more efficient
	@cached
	def getNumPickupsNearLocation(self, lat, lon, startTime, endTime):
		return int(TaxiPickup.select().where(self.isClose(lat, lon, TaxiPickup, 0.00224946357) & TaxiPickup.time.between(startTime, endTime)).count())

	# TODO: Use MySQL's Spatial Values to make this more efficient
	@cached
	def getNumDropoffsNearLocation(self, lat, lon, startTime, endTime):
		return int(TaxiDropoff.select().where(self.isClose(lat, lon, TaxiDropoff, 0.00224946357) & TaxiDropoff.time.between(startTime, endTime)).count())

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

		return date, datetime.time(hour, minute)

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
			eventDate, eventTime = self.parseTimeStr(timeStrList)

			# If no explicit eventTime is given, try to infer it from the description
			if eventTime == None:
				isAccurate = False
				eventTime = self.inferTime(eventDict['description'])
				if eventTime == None:
					eventTime = datetime.time()
				else:
					isInferred = True

			eventDict[timeField] = datetime.datetime.combine(eventDate, eventTime).strftime('%Y-%m-%d %H:%M:%S')

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

	def tweetDictToSQLStrings(self, tweetDict):
		html_parser = HTMLParser()
		SQLDict = {}

		# Populate the tweetDict. Use the get method to ensure no KeyErrors are raised
		text = tweetDict.get('text')
		if text == None:
			SQLDict['text'] = None
			SQLDict['mentions_taxi'] = False
		else:
			SQLDict['text'] = html_parser.unescape(text)
			text = SQLDict['text'].lower()
			SQLDict['mentions_taxi'] = 'taxi' in text or 'cab' in text

		coords = tweetDict.get('coordinates')
		if coords == None or coords == 'None':
			SQLDict['longitude'] = None
			SQLDict['latitude'] = None
		else:
			SQLDict['longitude'] = coords.get('coordinates')[0]
			SQLDict['latitude'] = coords.get('coordinates')[1]

		created_at = tweetDict.get('created_at')
		if created_at == None:
			SQLDict['created_at'] = None
		else:
			t = time.strptime(created_at.replace('+0000', ''), '%a %b %d %H:%M:%S %Y')
			SQLDict['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', t)

		SQLDict['favorited'] = tweetDict.get('favorited')

		SQLDict['tweet_id'] = tweetDict.get('id')

		place = tweetDict.get('place')
		if place == None or place == 'None':
			SQLDict['place_id'] = None
		else:
			SQLDict['place_id'] = place.get('id')

		SQLDict['retweet_count'] = tweetDict.get('retweet_count')

		SQLDict['source'] = tweetDict.get('source')

		user = tweetDict.get('user')
		if user == None:
			SQLDict['user_id'] = None
		else:
			SQLDict['user_id'] = user.get('id')

		return SQLDict

	# Adds the tweet data to the db
	def addTweets(self, tweetDicts):
		# Paginate so the queries don't get too long
		index = 0
		insertsPerQuery = 10000
		tweetStrings = [0] * insertsPerQuery
		while len(tweetStrings) == insertsPerQuery:
			tweetStrings = [self.tweetDictToSQLStrings(tweetDict) for tweetDict in (tweetDicts.next() for i in xrange(insertsPerQuery))]
			fields = tweetStrings[0].keys()
			flattenedTweetStrings = [sqlStrings[field] for sqlStrings in tweetStrings for field in fields]

			toReplace = ','.join(['(%s)' % (','.join(['%s'] * len(fields)))] * (len(flattenedTweetStrings) / len(fields)))
			
			database.execute_sql('INSERT INTO %s (%s) VALUES %s' % ('tweet', ','.join(fields), toReplace), flattenedTweetStrings)

			index += insertsPerQuery

	@cached
	def getNumTweetsNearLocation(self, latitude, longitude, startTime, endTime, distInMeters=250):
		dist = self.metersToCoordDist(distInMeters)
		return Tweet.select().where(self.isClose(latitude, longitude, Tweet, dist) & (Tweet.created_at.between(startTime, endTime))).count()

	@cached
	def getNumTweetsNearLocationMentioningTaxi(self, latitude, longitude, startTime, endTime, distInMeters=250):
		dist = self.metersToCoordDist(distInMeters)
		return Tweet.select().where(self.isClose(latitude, longitude, Tweet, dist) & (Tweet.mentions_taxi == 1) & (Tweet.created_at.between(startTime, endTime))).count()

	@cached
	def getNumTweetsMentioningTaxi(self, startTime, endTime):
		return Tweet.select().where((Tweet.mentions_taxi == 1) & (Tweet.created_at.between(startTime, endTime))).count()

	def addDicts(self, table, dicts, dictToSQLString):
		# Paginate so the queries don't get too long
		index = 0
		insertsPerQuery = 10000
		dictStrings = [0] * insertsPerQuery
		while len(dictStrings) == insertsPerQuery:
			dictStrings = [dictToSQLString(objectDicts) for objectDicts in (dicts.next() for i in xrange(insertsPerQuery))]
			fields = dictStrings[0].keys()
			flattenedDictStrings = [sqlStrings[field] for sqlStrings in dictStrings for field in fields]

			toReplace = ','.join(['(%s)' % (','.join(['%s'] * len(fields)))] * (len(flattenedDictStrings) / len(fields)))

			database.execute_sql('INSERT INTO %s (%s) VALUES %s' % (table, ','.join(fields), toReplace), flattenedDictStrings)

			index += insertsPerQuery

	def busDictToSQLStrings(self, TDict):
		SQLDict = {}

		# SQLDict['device_id'] = TDict['DEVICEID']
		# SQLDict['ticket_type'] = TDict['TICKETTYPE']
		# SQLDict['device_class_id'] = TDict['DEVICECLASSID']
		SQLDict['date'] = TDict['ScheduleDate']
		SQLDict['datetime'] = TDict['CREATEDATE'][:-4]
		SQLDict['route_station'] = TDict['RouteStation']

		return SQLDict

	def TDictToSQLStrings(self, TDict):
		SQLDict = {}

		# SQLDict['ticket_type'] = TDict['TicketTypeName']
		SQLDict['origin'] = TDict['Origin']
		SQLDict['destination'] = TDict['Destination']
		# SQLDict['trips'] = TDict['Trips']

		try:
			SQLDict['date'] = TDict['ScheduleDate']
			SQLDict['datetime'] = TDict['EntryDateTime']
			# SQLDict['next_trip_date'] = TDict['NextTripDateTime'][:-4]
		except KeyError:
			SQLDict['date'] = TDict['OrderDate']
			SQLDict['datetime'] = TDict['CreateDate'][:-4]
			# SQLDict['next_trip_date'] = TDict['NextTripDate'][:-4]
			
		return SQLDict

	def stopDictToSQLStrings(self, TDict):
		SQLDict = {}

		SQLDict['loc_id'] = TDict['locID']
		SQLDict['is_station'] = TDict['isStation']
		SQLDict['direction'] = TDict['Direction']
		SQLDict['next_loc_id'] = TDict['nextLocID']
		SQLDict['notes'] = TDict['Notes']
		SQLDict['longitude'] = TDict['Longitude']
		SQLDict['latitude'] = TDict['Latitude']
		SQLDict['next_station'] = TDict['nextStn']
		SQLDict['prev_station'] = TDict['prevStn']
		SQLDict['station'] = TDict['Station']
		SQLDict['sort_order'] = TDict['SortOrder']
		SQLDict['line'] = TDict['Line']
		SQLDict['circuit_number'] = TDict['CircuitNumber']
		SQLDict['heading'] = TDict['Heading']
		SQLDict['loc_type'] = TDict['LocType']
			
		return SQLDict

	# Adds the bus ride data to the db
	def addTRides(self, TDicts):
		self.addDicts('tride', TDicts, self.TDictToSQLStrings)

	# Adds the bus ride data to the db
	def addBusRides(self, busDicts):
		self.addDicts('busride', busDicts, self.busDictToSQLStrings)

	def addStops(self, stopDicts):
		self.addDicts('stop', stopDicts, self.stopDictToSQLStrings)

	@cached
	def getXClosestStations(self, latitude, longitude, x, isStation):
		return [stop.station for stop in Stop.select().where(Stop.is_station == isStation).order_by(self.getDist(latitude, longitude, Stop).asc()).limit(x)]

	@cached
	def getNumTRidesFromStation(self, station, startTime, endTime):
		return TRide.select().where((TRide.origin == station) & TRide.datetime.between(startTime, endTime)).count()

	@cached
	def getNumTRidesToStation(self, station, startTime, endTime):
		return TRide.select().where((TRide.destination == station) & TRide.datetime.between(startTime, endTime)).count()

	@cached
	def getNumTRidesFromXClosestStations(self, latitude, longitude, startTime, endTime, x):
		return sum(self.getNumTRidesFromStation(station, startTime, endTime) for station in self.getXClosestStations(latitude, longitude, x, True))

	@cached
	def getNumTRidesToXClosestStations(self, latitude, longitude, startTime, endTime, x):
		return sum(self.getNumTRidesToStation(station, startTime, endTime) for station in self.getXClosestStations(latitude, longitude, x, True))


if __name__ == '__main__':
	with DB() as db:
		db.createTables()
