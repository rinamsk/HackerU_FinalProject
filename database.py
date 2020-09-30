# HackerU Final Project
# Database
import sqlite3
import pandas as pd



class DB:

	def __init__(self):
		self.conn = sqlite3.connect('flats.db', check_same_thread=False)
		self.cursor = self.conn.cursor()

	def new_session(self, log_mode = False):
		self.deleteTMPTables()
		print('  deleteTMPTables done') if log_mode else None
		self.createFlat00Table()
		print('  '+'createFlat00Table'+' done') if log_mode else None
		self.createFlatTable()
		print('  '+'createFlatTable'+' done') if log_mode else None
		self.CreateAppViews()
		print('  '+'CreateAppViews'+' done') if log_mode else None
		
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

	def setNumbers(self):
		self.cursor.execute(
			'''
			UPDATE  flat 
			SET     price = to_number(price),
					price_per_meter = to_number(price_per_meter)

			'''
			)
		self.conn.commit()

	def setEndDttm(self):
		self.cursor.execute(
			'''
			UPDATE flat
			SET    end_dttm = datetime('2999-12-31 23:59:59')
			WHERE  end_dttm = '2020-09-30 19:02:50';
			--WHERE end_dttm IS NULL;
			'''
			)
		self.conn.commit()

	def CreateAppViews(self, viewName=None, dropView=False):
		if viewName is None or viewName == 'v_flat_price_stat':
			if dropView:
				self.cursor.execute(
				'''
				DROP VIEW if exists v_flat_price_stat;
				'''
				)

			self.cursor.execute(
				'''-- Изменение цены квартиры
				create view if not exists v_flat_price_stat AS
				SELECT 	flat_curr.id 							as curr_id,
						flat_prev.id 							as prev_id, 
						flat_curr.ext_key						as ext_key,
						flat_curr.city							as city,
						flat_curr.metro_station					as metro_station,
						CAST(flat_prev.price as int) 			as prev_price,
						flat_curr.price 						as curr_price,
						cast(flat_prev.price_per_meter as int) 	as prev_price_per_meter,
						cast(flat_curr.price_per_meter as int) 	as curr_price_per_meter,
						cast(flat_curr.price as int) - cast(flat_prev.price as int)
																as price_res,
						flat_curr.start_dttm					as curr_start_dttm,
						flat_curr.end_dttm						as curr_end_dttm
				FROM	flat flat_curr
				LEFT JOIN flat flat_prev ON  flat_curr.ext_key = flat_prev.ext_key
										 AND flat_prev.end_dttm = flat_curr.start_dttm
				WHERE	current_timestamp BETWEEN flat_curr.start_dttm AND flat_curr.end_dttm;
				'''
				)

		if viewName is None or viewName == 'v_flat_lower':
			if dropView:
				self.cursor.execute(
				'''
				DROP VIEW if exists v_flat_lower;
				'''
				)

			self.cursor.execute(
				'''--Квартиры, которые подешевели
				create view if not exists v_flat_lower AS
				SELECT 	*
				FROM	v_flat_price_stat
				WHERE	price_res < 0; -- Здесь отсекаем новые квартиры, так как нет NVL(0)
				'''
				)

		if viewName is None or viewName == 'v_area_higher':
			if dropView:
				self.cursor.execute(
				'''
				DROP VIEW v_area_higher;
				'''
				)

			self.cursor.execute(
				'''-- Районы, которые дорожают
				create  view if not exists v_area_higher AS
				SELECT 	metro_station, sum(price_res) as price_res
				FROM	v_flat_price_stat
				--WHERE   prev_id IS NOT NULL
				GROUP BY metro_station
				HAVING  sum(price_res) > 0
				;
				'''
				)

		if  viewName is None or viewName == 'v_new_flats':
			if dropView:
				self.cursor.execute(
				'''
				DROP VIEW v_new_flats;
				'''
				)

			self.cursor.execute(
				'''-- Новые квартиры
				create view if not exists v_new_flats AS
				SELECT 	*
				FROM	v_flat_price_stat
				WHERE   prev_id IS NULL
				AND     curr_start_dttm = (SELECT max(t.curr_start_dttm) FROM v_flat_price_stat t)
				;
				'''
				)

	def printRepHeader(self, print_str_=''):
		header_len = len(print_str_) + 8
		print('='*header_len)
		print('='*3 + ' '+ print_str_ + ' ' + '='*3) #if len(print_str_) > 0 else None
		print('='*header_len)

	def getRep(self, repNum=None):
		repList = ['v_flat_price_stat', 'v_flat_lower', 'v_area_higher', 'v_new_flats']
		headerList = ['Изменение цены', 'Квартиры, которые подешевели', 'Районы, которые дорожают', 'Новые квартиры']
		if (repNum is None) or not (0 <= repNum < len(repList)):
			print('Ошибка номера отчета:', repNum) if not repNum is None else None
			print('Доступный список отчетов:')
			for i in range(len(repList)):
				print(i, repList[i])
			return repList
		else:	
			self.printRepHeader(headerList[repNum])	
			repData = self.readTable(repList[repNum])
			for row in repData:
				print(row)
			return repData

	def createTableNewRows(self, log_mode=False):
		self.cursor.execute('''
			CREATE TABLE flat_2_insert AS
			SELECT t0.*
			FROM   flat_00 t0
			LEFT JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE  t.ext_key IS NULL;
			''')
		print('createTableNewRows done') if log_mode else None

	def checkTableNewRows(self):
		sql = '''
			SELECT t0.*
			FROM   flat_00 t0
			LEFT JOIN v_flat_curr t ON t0.ext_key = t.ext_key
			WHERE  t.ext_key IS NULL
			and    t0.ext_key = 217937882
			;
			'''
		self.cursor.execute(sql)
		return self.cursor.fetchall()


	def createTableUpdateRows(self, log_mode=False):
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
		print('createTableUpdateRows done') if log_mode else None

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
			--and    t0.ext_key = 217937882
			;
			'''
		self.cursor.execute(sql)
		return self.cursor.fetchall()

	def createTableDeleteRows(self, log_mode=False):
		self.cursor.execute(
			'''
			CREATE TABLE flat_2_delete AS
			SELECT t.*
			FROM   v_flat_curr t
			LEFT JOIN flat_00 t0 ON t0.ext_key = t.ext_key
			WHERE  t0.ext_key IS NULL
			AND    ifnull((SELECT count(1) as cnt FROM flat_00), 0) <> 0
			;
			''')

		print('createTableUpdateRows done') if log_mode else None

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
		self.createTableNewRows(log_mode=True)
		print('  '+'createTableNewRows...'+' done') if log_mode else None
		self.createTableUpdateRows(log_mode=True)
		print('  '+'createTableUpdateRows...'+' done') if log_mode else None
		self.createTableDeleteRows(log_mode=True)
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
		sql += f'WHERE {colName} = {ColValue}'
		sql += f';'
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

	def readTable(self, tableName, colName=None, ColValue=None):
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
	print('  deleteTMPTables' + 'done')
	db.createFlat00Table()
	print('  '+'createFlat00Table'+' done')
	db.createFlatTable()
	print('  '+'createFlatTable'+' done')
	db.CreateAppViews()
	print('  '+'CreateAppViews'+' done')

	db.createTableNewRows()
	print('  '+'createTableNewRows'+' done')
	db.createTableUpdateRows()
	print('  '+'createTableUpdateRows'+' done')
	db.createTableDeleteRows()
	print('  '+'addTableDeleteRows'+' done')
	db.updateFlatTable()
	print('  '+'updateAutoTable'+' done')


	