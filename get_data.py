import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import database
import re

db = database.DB()

soup_list = {
	
	'main_block'     : '.c6e8ba5398--main-container--1FMpY',
	'metro_station'  : '.c6e8ba5398--underground-name--1efZ3',
	'distance'       : '.c6e8ba5398--remoteness--3bptF',
	'price'          : '[data-name="TopPrice"] div',
	'price_per_metr' : '.c6e8ba5398--term--3kvtJ',
	'address'        : '.c6e8ba5398--address-links--1tfGW',
	'city'           : '.c6e8ba5398--link-container--1WHHu',
	'description'    : '.c6e8ba5398--container--F3yyv c6e8ba5398--info-section--Sfnx- c6e8ba5398--titled-description--1OX7l',
	'href'           : '.c6e8ba5398--header--1fV2A'
}

flat_attr = {
	'ext_id'         : None,
	'city'           : '',
	'metro_station'  : '',
	'distance'       : '',
	'address'        : '',
	'price'          : '',
	'price_per_metr' : '',
	'description'    : '',
	'room_square'    : '',
	'room_number'    : None,
	'sold'           : '',
	'href'           : ''
	}

def getExtID(href):
	res = re.findall(r'\d{3,}', href)
	return int(res[0])

def getData(firstPage=1, lastPage=0, log_mode = True):
	print("Поиск объявлений о продаже квартир в Москве")
	print('***Расширенное логирование включено***') if log_mode else None
	
	print('Connected to local DataBase') if log_mode else None
	print(log_mode)
	db.new_session(log_mode = log_mode)

	page_index = firstPage
	while True:
		print("Page {}".format(page_index))
		url = 'https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=1'
		#print(url.format(page_index))
		headers = {'user-agent': 'my-app/0.0.1'}
		result = requests.get(url.format(page_index), headers = headers)

		soup = BeautifulSoup(result.content, 'html.parser')
		
		flats_list = soup.select(soup_list['main_block'])


		if (len(flats_list) == 0) or (page_index > lastPage and lastPage != 0):
			print('end of searching on page', page_index)
			print('len(flats_list) == 0') if len(flats_list) == 0 else None
			print('age_index > lastPage and lastPage != 0') if page_index > lastPage and lastPage != 0 else None
			 
			break
		print('Page = ', page_index)

		for flat in flats_list:
			flat_attr = {}
			print(flat.text) if log_mode else None
			print('-'*20) if log_mode else None
			flat_attr['city'] = flat.select_one(soup_list['city']).text
			flat_attr['metro_station'] = flat.select_one(soup_list['metro_station']).text
			flat_attr['distance'] = flat.select_one(soup_list['distance']).text
			flat_attr['address'] = flat.select_one(soup_list['address']).text
			flat_attr['price'] = re.sub(r'\D', '', flat.select_one(soup_list['price']).text)
			flat_attr['price_per_metr'] = re.sub(r'\D', '', flat.select_one(soup_list['price_per_metr']).text)
			flat_attr['description'] = ''
			flat_attr['room_square'] = ''
			flat_attr['room_number'] = 0
			flat_attr['sold'] = 0
			flat_attr['href'] = flat.select_one(soup_list['href']).get('href')
			flat_attr['ext_id'] = getExtID(flat_attr['href'])

			print(flat_attr['ext_id']) if log_mode else None
			print(flat_attr['city']) if log_mode else None
			print(flat_attr['metro_station']) if log_mode else None
			print(flat_attr['distance']) if log_mode else None
			print(flat_attr['address']) if log_mode else None
			print(flat_attr['price']) if log_mode else None
			print(flat_attr['price_per_metr']) if log_mode else None
			print(flat_attr['description']) if log_mode else None
			print(flat_attr['room_square']) if log_mode else None
			print(flat_attr['room_number']) if log_mode else None
			print(flat_attr['sold']) if log_mode else None
			print(flat_attr['href']) if log_mode else None

			print(type(flat_attr))

			db.load_data(flat_attr = flat_attr)

			print('='*20) if log_mode else None
		page_index += 1
		print('<'*10, page_index )
	db.processData(log_mode=log_mode)
	print("End of load data")

def getFlatData(flatNum = '219255833'):
	return db.readTable('flat', 'ext_key', flatNum)

def getRep(repNum = 0):
	return db.getRep(repNum)
	