import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

print("Поиск объявлений о продаже квартир в Москве")

#url = 'http://mdk-arbat.ru/catalog/'

i = 1
i_max = 3
while True:
	print("Page {}".format(i))
	url = 'https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=1'
	print(url.format(i))
	result = requests.get(url.format(i))

	soup = BeautifulSoup(result.content, 'html.parser')

	flats_list = soup.select('div.c6e8ba5398--address-links--1tfGW')

	if (len(flats_list) == 0) or (i > i_max):
		print('end of searching')
		break

	for flat in flats_list:
		print(flat.text)
		#print(flat.select_one('.tg-booktitle h3 a').text)
		#print(flat.select_one('.tg-bookwriter a').text)
		#print(flat.select_one('.tg-bookprice').text)
		print('-'*20)
	i += 1
	print('<'*10, i )

print("End of programm")
	
	