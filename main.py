import os, sys, glob, itertools
import gzip
import csv
import sqlite3
import requests
from twitch import TwitchHelix
from dotenv import load_dotenv
from flask import *

app = Flask(__name__)

class TwitchHelper:
	def __init__(self, argv):
		self.__argv = argv
		self.__merge = False
		self.__fill_names = False
		self.__truncate = False
		self.__web_interface = False

		self.__data_path = ""
		self.__output_db = ""
		self.__archive_files = []
		self.__processed_file_count = 0
		self.__total_file_count = 0

		self.__twitch_client = None
		self.__default_user_name = "..."
		self.__users = {}
		self.__user_views = {}

		load_dotenv()
		self.__parse_args()
		self.__connect_to_twitch()

	def __parse_args(self):
		if (len(self.__argv) < 2 or self.__argv[1] == "--help"):
			print("Usage: py {0} [--data_path path] [--output_db db] [--merge] [--fill_names] [--truncate] [--web]".format(sys.argv[0]))
			sys.exit(1)

		if (self.__argv.__contains__("--data_path")):
			if (self.__argv.index("--data_path") + 1 > len(self.__argv)):
				print("Error: data_path is missing.")
				sys.exit(1)
			self.__data_path = "{0}{1}{2}".format(os.getcwd(), os.path.sep, self.__argv[self.__argv.index("--data_path") + 1])
			print("data_path: {0}".format(self.__data_path))

		if (self.__argv.__contains__("--output_db")):
			if (self.__argv.index("--output_db") + 1 > len(self.__argv)):
				print("Error: output_db is missing.")
				sys.exit(1)
			self.__output_db = "{0}{1}{2}".format(os.getcwd(), os.path.sep, self.__argv[self.__argv.index("--output_db") + 1])
			print("output_db: {0}".format(self.__output_db))

		if (self.__argv.__contains__("--merge")):
			self.__merge = True
			print("Merge mode enabled.")

		if (self.__argv.__contains__("--fill_names")):
			self.__fill_names = True
			print("Fill names mode enabled.")

		if (self.__argv.__contains__("--truncate")):
			self.__truncate = True
			print("Truncate mode enabled.")

		if (self.__argv.__contains__("--web")):
			self.__web_interface = True
			print("Web interface enabled.")

	def __connect_to_twitch(self):
		__cid, __token, __secret = os.getenv("TWITCH_CLIENT_ID"), os.getenv("TWITCH_OAUTH_TOKEN"), os.getenv("TWITCH_CLIENT_SECRET")
		if (not __cid or not __token or not __secret):
			print("Error: twitch client id, oauth token or client secret is missing in .env file.")
			sys.exit(1)

		try:
			self.__twitch_client = TwitchHelix(__cid, __token, __secret)
		except Exception as e:
			print("TwitchClient object create failed with error: {0}".format(e))

	def __get_users_by_id_list(self, ids):
		# print("Getting users by id list... Count: {0}".format(len(ids)))

		if (len(ids) == 0):
			return []
		
		try:
			return self.__twitch_client.get_users(ids=ids)
		except requests.exceptions.HTTPError as e:
			print("get_users_by_id_list failed with error: '{0}'".format(e));
			return []

	def __check_file_integrity(self, path):
		for root, dirs, files in os.walk(path):
			if (files and len(files) == 1 and files[0] == "all_revenues.csv.gz"):
				return True
		return True

	def __check_path_integrity(self, path):
		for root, dirs, files in os.walk(path):
			if (dirs and dirs[0] == "2019"):
				__samplePath = "{0}{1}{2}{1}08{1}28".format(root, os.path.sep, dirs[0])
				if (self.__check_file_integrity(__samplePath)):
					return True
				else:
					print("File integrity check failed.")
					return False
			else:
				print("Folder structure is invalid.")
				return False
		return False

	def __create_connection(self):
		try:
			print("SQLite3 version: {0}".format(sqlite3.version))

			# Create output database
			print("Creating output database...")
			self.__conn = sqlite3.connect(self.__output_db)

			# Create cursor
			print("Creating cursor...")
			self.__c = self.__conn.cursor()
			return True
		except Exception as e:
			print("Error: {0}".format(e))
			return False

	def __create_tables(self):
		try:
			# Global sqlite configrations
			self.__c.execute("PRAGMA journal_mode = MEMORY")
			self.__c.execute("PRAGMA synchronous = OFF")

			# Drop old table if exists
			self.__c.execute(f'''DROP TABLE IF EXISTS earnings''')

			# Create new tables
			self.__c.execute(f'''
				CREATE TABLE "earnings" (
					"user_id"	INTEGER NOT NULL,
					"user_name"	TEXT NOT NULL DEFAULT '',
					"month"	INTEGER NOT NULL,
					"year"	INTEGER NOT NULL,
					"ad_share"	NUMERIC,
					"sub_share"	NUMERIC,
					"bit_share"	NUMERIC,
					"bit_developer_share"	NUMERIC,
					"bit_extension_share"	NUMERIC,
					"prime_sub_share"	NUMERIC,
					"bit_share_ad"	NUMERIC,
					"fuel_rev"	NUMERIC,
					"bb_rev"	NUMERIC,
					"total_gross"	NUMERIC,
					"view_count"	NUMERIC,
					PRIMARY KEY("user_id","month","year")
				)'''
			)

			self.__conn.commit()
			return True
		except sqlite3.Error as e:
			print("sqlite3.connect failed with error: {0}".format(e))
			return False

	def __find_archive_files(self):
		for filename in glob.iglob(self.__data_path + '**/**', recursive=True):
			if (filename.endswith(".csv.gz")):
				print("Find archive file: [{0}] :: {1}".format(self.__total_file_count, filename))

				self.__total_file_count += 1
				self.__archive_files.append(filename)
		
		print("Total archive files: {0}".format(self.__total_file_count))
		return self.__total_file_count > 0

	def __process_archive(self, filename):
		# get gross revenue
		def __calculate_gross_revenue(revenus):
			__counter = 0;

			gross_revenue = 0.0;
			for revenue in revenus:
				if __counter >= 2 and __counter <= 10:
					gross_revenue += float(revenue)
				__counter += 1
			
			return gross_revenue

		print("Processing archive file: [{0}] :: {1}".format(self.__processed_file_count, filename))

		# Open the file
		try:
			f = gzip.open(filename, "rt")
			csv_reader = csv.reader(f)
		except OSError as e:
			print("gzip.open({0}) failed with error: {1}".format(filename, e))
			sys.exit()

		# Skip the first header line
		next(csv_reader)

		# Read the file
		__once, __month, __year = 0, 0, 0
		for row in csv_reader:
			# Sanity check
			if (len(row) < 11 or not row[11]):
				print("[1] Invalid row: {0}".format(row))
				continue

			if (__once == 0):
				__once = 1
			elif (__once == 1): # convert date only once
				__date = row[11].split('/')
				__month, __year = __date[0], __date[2]

			# Sanity check part 2
			if (__month == 0 or __year == 0):
				print("[2] Invalid row: {0}".format(row))
				continue

			__sum = __calculate_gross_revenue(row)
			# Check she has a valid revenue
			if (__sum == 0):
				continue

			# Reserve user id from user table
			if (row[0] not in self.__users):
				self.__users[row[0]] = self.__default_user_name
			
			# Check if user exists
			__exist = None
			try:
				self.__c.execute("SELECT user_id FROM earnings WHERE user_id = ? AND month = ? AND year = ?", (row[0], __month, __year))
				__exist = self.__c.fetchone() is not None
			except sqlite3.Error as e:
				print("sqlite3 query failed with error: {0}".format(e))
				sys.exit()

			# Insert into database
			try:
				if (not __exist):
					self.__c.execute("INSERT INTO earnings(user_id, user_name, month, year, ad_share, sub_share, bit_share, bit_developer_share, bit_extension_share, prime_sub_share, bit_share_ad, fuel_rev, bb_rev, total_gross) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], self.__default_user_name, __month, __year, row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], __sum))
				else:
					self.__c.execute("UPDATE earnings SET ad_share = ad_share + ?, sub_share = sub_share + ?, bit_share = bit_share + ?, bit_developer_share = bit_developer_share + ?, bit_extension_share = bit_extension_share + ?, prime_sub_share = prime_sub_share + ?, bit_share_ad = bit_share_ad + ?, fuel_rev = fuel_rev + ?, bb_rev = bb_rev + ?, total_gross = total_gross + ? WHERE user_id = ? AND year = ? AND month = ?", (row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], __sum, row[0], __year, __month))
			except sqlite3.Error as e:
				print("sqlite3.execute failed with error({0}): {1}".format(e.__class__, e))
				sys.exit()

		# Close the file
		f.close()

		# Commit the changes
		try:
			self.__conn.commit()
		except sqlite3.Error as e:
			print("sqlite3.commit failed with error: {0}".format(e))
			sys.exit()
		
		# Increment processed file count
		self.__processed_file_count += 1

		# Update the progress
		print("Processed {0}/{1} files.".format(self.__processed_file_count, self.__total_file_count))

	def __user_id_chunks(self, data, chunksize):
		it = itertools.cycle(range(chunksize))
		chunks = [dict() for _ in range(chunksize)]

		for k, v in data.items():
			chunks[next(it)][k] = v

		return chunks

	def __get_unfilled_user_table_length(self):
		try:
			self.__c.execute("SELECT COUNT(*) FROM earnings WHERE user_name = '{0}'".format(self.__default_user_name))
			return self.__c.fetchone()[0]
		except sqlite3.Error as e:
			print("sqlite3.execute failed with error: {0}".format(e))
			return 0

	def __load_users(self, limit):
		print("Loading users...")

		# Get all users
		try:
			self.__c.execute("SELECT user_id, user_name FROM earnings WHERE user_name = '{0}' LIMIT {1}".format(self.__default_user_name, limit))
			__users = self.__c.fetchall()
			for user in __users:
				self.__users[user[0]] = user[1]
		except sqlite3.Error as e:
			print("sqlite3 query failed with error: {0}".format(e))
			sys.exit()

	def __fetch_user_names(self):
		__unknown_users = {k: v for k, v in self.__users.items() if v == self.__default_user_name}
		print("Total user size: {0} Filtered unknown user size: {1}".format(len(self.__users), len(__unknown_users)))

		if (len(__unknown_users)):
			# Split user ids into chunks(which is limited with 100 by Twitch API) see: https://dev.twitch.tv/docs/api/reference#get-users
			__chunk_size = int(len(__unknown_users) / 100) if len(__unknown_users) > 100 else 1
			print("Chunk size: {0}".format(__chunk_size))
		
			# Fetch user names from Twitch API
			__chunks = self.__user_id_chunks(__unknown_users, __chunk_size + 1)
			print("Chunk count: {0}".format(len(__chunks)))

			__idx = 0
			for chunk in __chunks:
				__idx += 1
				print("Fetching user names from Twitch API for chunk: {0}".format(__idx))

				# Get user names of chunk
				__users = self.__get_users_by_id_list(chunk.keys())

				# Check if user names are fetched
				if (not __users):
					print("Failed to get user names of chunk: {0}".format(chunk))
					return False

				# Add user names to user table
				for user in __users: 
					if ("display_name" in user):
						self.__users[user['id']] = user['display_name']
						self.__user_views[user['id']] = user['view_count']
						# print("User: {0} name: {1}".format(user['id'], user['display_name']))
					else:
						print("Failed to get user name of user: {0}".format(user))
						return False

		return True

	def __fill_user_table(self):
		# Fill user table
		__idx = 0
		for __user_id, __user_name in self.__users.items():
			if (__user_name != self.__default_user_name):
				__idx += 1
				print("Filling user informations: {0}".format(__idx))

				try:
					self.__c.execute("UPDATE earnings SET user_name = ? AND view_count = ? WHERE user_id = ?", (__user_name, self.__user_views[__user_id], __user_id))
				except sqlite3.Error as e:
					print("sqlite3.execute(2) failed with error({0}): {1}".format(e.__class__, e))
					return False

		# Commit the changes
		try:
			self.__conn.commit()
		except sqlite3.Error as e:
			print("sqlite3.commit(2) failed with error: {0}".format(e))
			return False

		return True

	def run(self):
		if (self.__merge):
			if (self.__truncate and os.path.exists(self.__output_db)):
				os.remove(self.__output_db)

			# Check the database is already created
			if (not os.path.exists(self.__output_db)):
				# Merge csv files to database
				print("Database not found, creating database...")

				# Check if the data path is valid
				if self.__check_path_integrity(self.__data_path):
					print("Path and folder structure are valid.")
				else:
					print("Path and folder structure are invalid.")
					return 1

				print("Processing...")

				# Create output path
				if not os.path.exists("{0}{1}output".format(os.getcwd(), os.path.sep)):
					try:
						os.mkdir("{0}{1}output".format(os.getcwd(), os.path.sep))
					except OSError as e:
						print("os.mkdir failed with error: {0}".format(e))
						return 2

				# Create connection to database
				if (self.__create_connection() == False):
					print("Failed to create database.")
					return 3

				# Create output database and tables
				if (self.__create_tables() == False):
					print("Failed to create output database.")
					return 4

				# Find archive files
				if (self.__find_archive_files() == False):
					print("Failed to find archive files.")
					return 5

				# Read and process all archive files in the data path
				for archive in self.__archive_files:
					self.__process_archive(archive)		
				
				# Close the database
				try:
					self.__conn.close()
				except sqlite3.Error as e:
					print("sqlite3.close failed with error: {0}".format(e))

				print("Done. Output database is created in: {0}{1}output".format(os.getcwd(), os.path.sep))
			else:
				print("Database already exists.")
		
		if (self.__fill_names):
			# Create connection to database
			if (self.__create_connection() == False):
				print("Failed to create database.")
				return 6

			# Get user count from database
			__user_count = self.__get_unfilled_user_table_length()
			if (__user_count == 0):
				print("Failed to get user count from database.")
				return 7
			print("User count: {0}".format(__user_count))

			__step_size = 500000
			__step = int(__user_count / __step_size) if __user_count > __step_size else 1
			print("User count loop step count: {0} step size: {1}".format(__step, __step_size))
			for i in range(__step):
				# Load users
				if (self.__load_users(__step_size) == False):
					print("Failed to load users.")
					return 8

				# Fetches user names
				if (self.__fetch_user_names() == False):
					print("Failed to fetch user names.")
					return 9

				# Fill user table
				if (self.__fill_user_table() == False):
					print("Failed to fill user table.")
					return 10

			# Close the database
			try:
				self.__conn.close()
			except sqlite3.Error as e:
				print("sqlite3.close failed with error: {0}".format(e))

		if (self.__web_interface):
			print("Starting web interface...")
			app.run(debug = True)
		return 0

@app.route("/", methods = ["POST","GET"])
def index():
	try:
		if request.method == "POST":
			__name = request.form["name"]
			print("Searching for: {0}".format(__name))

			if (__name):
				if "search" in request.form:
						
					__db_filename = "{0}/output{1}output.db".format(os.getcwd(), os.path.sep)
					if (os.path.exists(__db_filename)):
						__con = sqlite3.connect(__db_filename)
						if (__con):
							rows = []
							total_earnings_ad, total_earning_sub, total_earning_bit, total_earning_prime, total_earning_gross = 0, 0, 0, 0, 0

							__c = __con.cursor()
							if (__c):
								__c.execute("SELECT * FROM earnings WHERE user_name LIKE ? ORDER BY year, month", ("%" + __name + "%",))
								rows = __c.fetchall()

								for row in range(len(rows)):
									rows[row] = list(rows[row])

									total_earnings_ad += rows[row][4]
									total_earning_sub += rows[row][5]
									total_earning_bit += rows[row][6]
									total_earning_prime += rows[row][9]
									total_earning_gross += rows[row][13]

									for i in range(len(rows[row])):
										if (type(rows[row][i]) == float):
											rows[row][i] = round(rows[row][i], 2)

								print("Found {0} rows.".format(len(rows)))

							__con.close()
							
							return render_template("index.html", rows = rows, total = [
								round(total_earnings_ad, 2),
								round(total_earning_sub, 2),
								round(total_earning_bit, 2),
								round(total_earning_prime, 2),
								round(total_earning_gross, 2)
							])
	except Exception as e:
		print("search_streamer failed with error: {0}". format(e))
	
	return render_template("index.html", rows = [], total = [])

if __name__ == "__main__":
	worker = TwitchHelper(sys.argv[1:])
	sys.exit(worker.run())
