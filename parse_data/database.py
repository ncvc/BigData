import datetime

from peewee import MySQLDatabase, Model, CharField, DateTimeField, IntegerField, BooleanField, TextField, DecimalField

database = MySQLDatabase('big_data', host='localhost', port=3306, user='root', passwd='')

# Database must use utf8mb4 for smileys and other such nonesense
# ALTER DATABASE hn CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


# Model definitions
class BaseModel(Model):
	class Meta:
		database = database

class TaxiPickup(BaseModel):
	trip_id = IntegerField()
	time = DateTimeField(formats='%m/%d/%Y %H:%M')
	address = TextField()
	longitude = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	latitude = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)

class TaxiDropoff(BaseModel):
	trip_id = IntegerField()
	time = DateTimeField(formats='%m/%d/%Y %H:%M')
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
	wdire = CharField(max_length=3, null=True)                            # Wind direction description (i.e., SW, NNE)
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
	time = DateTimeField(formats='%m/%d/%Y %H:%M')


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
	def create_tables(self):
		TaxiPickup.create_table()
		TaxiDropoff.create_table()
		Weather.create_table()

	# # Get all comments in the date range
	# # Note: Do manual pagination because MySQL LIMIT's OFFSET parameter is super slow when the offset gets large
	# def get_comments(self, startDate, endDate, resultsPerPage=10000):
	# 	adjustedEndDate = endDate - datetime.timedelta(1)
	# 	numPages = int(Story.select().where((Story.type == 'comment') & (Story.time.between(startDate, adjustedEndDate))).count() / resultsPerPage) + 1
	# 	lastId = 0
	# 	for page in xrange(numPages):
	# 		numNewComments = 0
	# 		for comment in Story.select().where((Story.id > lastId) & (Story.type == 'comment') & (Story.time.between(startDate, adjustedEndDate))).limit(resultsPerPage):
	# 			numNewComments += 1
	# 			yield comment
	# 		lastId = comment.id

	# 		if numNewComments < resultsPerPage:
	# 			break

	# # Adds the story data to the db
	# def add_story(self, storyData):
	# 	story = Story()

	# 	story.id = storyData.get('id')
	# 	story.kids = storyData.get('kids')
	# 	story.author = storyData.get('by')
	# 	story.text = storyData.get('text')
	# 	story.type = storyData.get('type')
	# 	story.parent = storyData.get('parent')
	# 	story.url = storyData.get('url')
	# 	story.title = storyData.get('title')
	# 	story.dead = storyData.get('dead')
	# 	story.deleted = storyData.get('deleted')
	# 	story.parts = storyData.get('parts')

	# 	score = storyData.get('score')
	# 	if isinstance(score, (int, long)):
	# 		story.score = score
	# 	else:
	# 		story.score = 0

	# 	time = storyData.get('time')
	# 	if time == None:
	# 		story.time = None
	# 	else:
	# 		story.time = datetime.datetime.fromtimestamp(time)

	# 	# Write the new row to the database
	# 	story.save(force_insert=True)


if __name__ == '__main__':
	with DB() as db:
		db.create_tables()
