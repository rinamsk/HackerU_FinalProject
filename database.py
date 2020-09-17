# HackerU Final Project
# Database
import sqlite3
import pandas as pd



class DB:

	def __init__(self):
		self.conn = sqlite3.connect('flats.db')
		self.cursor = self.conn.cursor()

	def new_session(self, log_mode = False):
		self.deleteTMPTables()
		print('  deleteTMPTables done') if log_mode else None
		self.createFlat00Table()
		print('  '+'createFlat00Table'+' done') if log_mode else None
		self.createFlatTable()
		print('  '+'createFlatTable'+' done') if log_mode else None

	def createFlat00Table(self):
		self.cursor.execute('''
				create table if not exists flat_00(
					ext_key			int,
					city			varchar2(60),
					metro_station	varchar2(60),
					distance		varchar2(30),
					address			varchar(256),
					price			varchar(128),
					price_per_meter	varchar(128),
					description		varchar(256),
					room_square		varchar2(30),
					room_number		int,
					sold			int,
					href            varchar2(100)
				   );
			''')

	def createFlatTable(self):
		self.cursor.execute('''
				create table if not exists flat(
					id 				integer primary key autoincrement,
					ext_key			int,
					city			varchar2(60),
					metro_station	varchar2(60),
					distance		varchar2(30),
					address			varchar(256),
					price			int,
					price_per_meter	int,
					description		varchar(256),
					room_square		varchar2(30),
					room_number		int,
					sold			int,
					href            varchar2(100),
					start_dttm		datetime default current_timestamp,
					end_dttm		datetime default (datetime('2999-12-31 23:59:59'))
				   );
			''')

		self.cursor.execute(
			'''
			create view if not exists v_flat_curr AS
			SELECT 	id,
					ext_key,
					city,
					metro_station,
					distance,
					address,
					price,
					price_per_meter,
					description,
					room_square,
					room_number,
					sold,
					href,
					start_dttm,
					end_dttm
			FROM	flat
			WHERE	current_timestamp BETWEEN start_dttm AND end_dttm;

			'''
			)

	def createTableNewRows(self):
		self.cursor.execute('''
			CREATE TABLE flat_2_insert AS
			SELECT t0.*
			FROM   flat_00 t0
			LEFT JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE  t.ext_key IS NULL;
			''')

	def checkTableNewRows(self):
		sql = '''
			SELECT t0.*
			FROM   flat_00 t0
			LEFT JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE  t.ext_key IS NULL
			and    t0.ext_key = 219255833
			;
			'''
		self.cursor.execute(sql)
		return self.cursor.fetchall()


	def createTableUpdateRows(self):
		self.cursor.execute(
			'''
			CREATE TABLE flat_2_update AS
			SELECT t0.*
			FROM   flat_00 t0
			INNER JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE NOT ( t0.city = t.city
						AND t0.metro_station = t.metro_station
						AND t0.distance = t.distance
						AND t0.address = t.address
						AND t0.price = t.price
						AND t0.price_per_meter = t.price_per_meter
						AND t0.description = t.description
						AND t0.room_square = t.room_square
						AND t0.room_number = t.room_number
						AND t0.sold = t.sold
						AND t0.href = t.href
				   );
			''')
		
	def checkTableUpdateRows(self):
		sql = '''
			SELECT t0.*
			FROM   flat_00 t0
			INNER JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE NOT ( t0.city = t.city
						AND t0.metro_station = t.metro_station
						AND t0.distance = t.distance
						AND t0.address = t.address
						AND t0.price = t.price
						AND t0.price_per_meter = t.price_per_meter
						AND t0.description = t.description
						AND t0.room_square = t.room_square
						AND t0.room_number = t.room_number
						AND t0.sold = t.sold
						AND t0.href = t.href
					)
			and    t0.ext_key = 219255833;
			'''
		self.cursor.execute(sql)
		return self.cursor.fetchall()

	def createTableDeleteRows(self):
		self.cursor.execute(
			'''
			CREATE TABLE flat_2_delete AS
			SELECT t.*
			FROM   v_flat_curr t
			LEFT JOIN flat_00 t0 ON t0.ext_key = t.ext_key
			WHERE  t0.ext_key IS NULL;
			''')

	def csv2sql(self, filePath):
		df = pd.read_csv(filePath)
		df.to_sql('auto_00', con=self.conn, if_exists='replace')

	def load_data(self, flat_attr = {}):
		str_sql = ("""
			INSERT INTO flat_00(
				ext_key, city, metro_station, distance, address,
				price, price_per_meter, description, room_square, room_number, sold, href)
			VALUES
			(""" +   str(flat_attr['ext_id'])        + ","  \
			     + "'" + flat_attr['city']           + "'," \
			     + "'" + flat_attr['metro_station']  + "'," \
			     + "'" + flat_attr['distance']       + "'," \
			     + "'" + flat_attr['address']        + "'," \
			     + "'" + flat_attr['price']          + "'," \
			     + "'" + flat_attr['price_per_metr'] + "'," \
			     + "'" + flat_attr['description']    + "'," \
			     + "'" + flat_attr['room_square']    + "'," \
			     +   str(flat_attr['room_number'])   + ","  \
			     +   str(flat_attr['sold'])          + ","  \
			     + "'" + flat_attr['href']           + "'"  \
			+ """)
			""")
		#print(str_sql)
		self.cursor.execute(str_sql)
		self.conn.commit()

	def updateFlatTable(self, log_mode = False):
		# DELETED ROWS
		print('  '+'* Starting to delete data...', end = '') if log_mode else None
		self.cursor.execute('''
			UPDATE flat
			SET    end_dttm = current_timestamp
			WHERE  ext_key IN
				(SELECT t2.ext_key FROM flat_2_delete t2)
			AND    end_dttm = (datetime('2999-12-31 23:59:59'));
			'''
		)
		print(' done.') if log_mode else None

		# CHANGED ROWS
		print('  '+'* Starting to update data...', end = '') if log_mode else None
		self.cursor.execute(
			'''
			UPDATE flat
			SET    end_dttm = current_timestamp
			WHERE  ext_key IN
				(SELECT t2.ext_key FROM flat_2_update t2)
			AND    end_dttm = (datetime('2999-12-31 23:59:59'));
			'''
		)

		self.cursor.execute(
			'''
			INSERT INTO flat
					(ext_key,
					 city, metro_station, distance,
					 address, price, price_per_meter,
					 description, room_square, room_number, sold, href
					)
			SELECT 	t2.ext_key,
					t2.city, t2.metro_station, t2.distance,
					t2.address, t2.price, t2.price_per_meter,
					t2.description, t2.room_square, t2.room_number, NULL as sold, t2.href
			FROM   	flat_2_update t2;
			'''
		)
		print(' done.') if log_mode else None

		# NEW ROWS
		print('  '+'* Starting to insert new data...', end = '') if log_mode else None
		self.cursor.execute(
			'''
			INSERT INTO flat
					(ext_key,
					 city, metro_station, distance,
					 address, price, price_per_meter,
					 description, room_square, room_number, sold, href
					)
			SELECT 	t2.ext_key, 
					t2.city, t2.metro_station, t2.distance,
					t2.address, t2.price, t2.price_per_meter,
					t2.description, t2.room_square, t2.room_number, NULL as sold, t2.href
			FROM   	flat_2_insert t2;
			'''
		)
		print(' done.') if log_mode else None
		self.conn.commit()
		print('  '+'Commited.') if log_mode else None

	def processData(self, log_mode):
		print('Starting to process data:') if log_mode else None
		self.createTableNewRows()
		print('  '+'createTableNewRows...'+' done') if log_mode else None
		self.createTableUpdateRows()
		print('  '+'createTableUpdateRows...'+' done') if log_mode else None
		self.createTableDeleteRows()
		print('  '+'createTableDeleteRows...'+' done') if log_mode else None
		self.updateFlatTable(log_mode=True)
		print('  '+'updateFlatTable'+' done') if log_mode else None
		print('End of processing data.') if log_mode else None
		


	def deleteTMPTables(self):
		self.cursor.execute(
			'''
			drop table if exists flat_00;
			'''
		)

		self.cursor.execute(
			'''
			drop table if exists flat_2_update;
			'''
		)
		self.cursor.execute(
			'''
			drop table if exists flat_2_insert;
			'''
		)
		self.cursor.execute(
			'''
			drop table if exists flat_2_delete;
			'''
		)
	def deleteMainTables(self, log_mode=False):
		self.cursor.execute(
			'''
			drop table if exists flat;
			'''
		)
		print('  '+'dropFlatTable'+' done') if log_mode else None
	
	def updateFlatAttr(self, colName, ColValue, colChangeName, colChangeValue):
		sql = f'UPDATE flat '
		sql += f'SET {colChangeName} = {colChangeValue} '
		sql += f'WHERE {colName} = {ColValue};'
		print(sql)
		self.cursor.execute(sql)
		self.conn.commit()


	def tableStat(self, tableName):
		sql = f'SELECT count(1) FROM {tableName};'
		self.cursor.execute(sql)
		return self.cursor.fetchall()

	def databaseStat(self):
		tableNameList = ['flat', 'flat_00', 'flat_2_update', 'flat_2_insert', 'flat_2_delete']
		dbStat = {}
		for table in tableNameList:
			dbStat[table] = self.tableStat(table)
		print(dbStat)

	def readTable(self, tableName, colName, ColValue):
		sql = f'SELECT * FROM {tableName}'
		if not(colName is None or ColValue is None):
			sql += f" WHERE {colName} = '{ColValue}'"
		sql += ';'
		print('Result of ' + sql)
		self.cursor.execute(sql)
		return self.cursor.fetchall()


if __name__ == '__main__':
	print('  init...')
	#fileURL = r'C:\\Python\\python_spec\\lesson_16\\auto_ru_parse\\results\\2020_09_08__13_26_29\\result_cleared.csv\\part-00000-4f9ae315-37b3-46ec-8e7a-b996b1a3db33-c000.csv'
	fileURL = 'data3.csv'
	print('='*10 + ' ' + fileURL + ' ' + '='*10)
	
	db = DB()
	print('  Connection done')

	db.deleteTMPTables()
	print('  deleteTMPTables done')
	db.csv2sql(fileURL) # Загрузка данных выгрузки в SQL
	print('  '+'csv2sql'+' done')
	db.createAutoTable()
	print('  '+'createAutoTable'+' done')
	db.createTableNewRows()
	print('  '+'createTableNewRows'+' done')
	db.createTableUpdateRows()
	print('  '+'createTableUpdateRows'+' done')
	db.addTableDeleteRows()
	print('  '+'addTableDeleteRows'+' done')
	db.updateAutoTable()
	print('  '+'updateAutoTable'+' done')



	print('_'*10 + 'auto_2_insert:' + '_'*10)
	for row in readTable('auto_2_insert'):
		print(row)

	print('_'*10 + 'auto_2_update:' + '_'*10)
	for row in readTable('auto_2_update'):
		print(row)

	print('_'*10 + 'auto_2_delete:' + '_'*10)
	for row in readTable('auto_2_delete'):
		print(row)

	print('_'*10 + 'auto:' + '_'*10)
	for row in readTable('auto'):
		print(row)
	