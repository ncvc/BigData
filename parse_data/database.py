import datetime

from peewee import MySQLDatabase, Model, CharField, DateTimeField, IntegerField, BooleanField, TextField, DecimalField

database = MySQLDatabase('big_data', host='localhost', port=3306, user='root', passwd='')

WEATHER_FIELD_LIST = ['heatindexm', 'windchillm', 'wdird', 'windchilli', 'hail', 'heatindexi', 'wgusti', 'thunder', 'pressurei', 'snow', 'pressurem', 'fog', 'vism', 'wgustm', 'tornado', 'hum', 'tempi', 'tempm', 'dewptm', 'rain', 'dewpti', 'precipm', 'wspdi', 'wspdm', 'visi']


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
		return self

	def __exit__(self, type, value, traceback):
		print 'DB.__exit__', type, value, traceback
		database.close()

	# Simple utility function to create tables
	def createTables(self):
		TaxiPickup.create_table()
		TaxiDropoff.create_table()
		Weather.create_table()

	# Adds the taxi data to the db
	def addTaxiPickup(self, pickupDict):
		pickup = TaxiPickup()

		pickup.trip_id = pickupDict['ID']
		pickup.time = datetime.datetime.strptime(pickupDict['DROPOFF_TIME'], '%Y-%m-%d %H:%M:%S')
		pickup.address = pickupDict['DROPOFF_ADDRESS']
		pickup.longitude = pickupDict['DROPOFF_LONG']
		pickup.latitude = pickupDict['DROPOFF_LAT']

		# Write the new row to the database
		pickup.save()

	# Adds the taxi data to the db
	def addTaxiDropoff(self, dropoffDict):
		dropoff = TaxiDropoff()

		dropoff.trip_id = dropoffDict['ID']
		dropoff.time = datetime.datetime.strptime(dropoffDict['DROPOFF_TIME'], '%m/%d/%Y %H:%M')
		dropoff.address = dropoffDict['DROPOFF_ADDRESS']
		dropoff.longitude = dropoffDict['DROPOFF_LONG']
		dropoff.latitude = dropoffDict['DROPOFF_LAT']

		# Write the new row to the database
		dropoff.save()

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


if __name__ == '__main__':
	with DB() as db:
		db.createTables()
