from flask import Flask
import json
from get_data import getData, getFlatData, getRep
import database

app = Flask(__name__)
db = database.DB()
		

@app.route('/test/<name>')
def testApp(name):
	return f'hello, {name}'


@app.route('/api/loadData/')
def loadData():
	try:
		getData(firstPage = 2, log_mode = True)
		return json.dumps({"status":'ok'})
	except Exception as e:
		return json.dumps({"status":'err', 'error_text': str(e)})


@app.route('/api/getRepList/')
def getRepList():
	return json.dumps(db.getRep())		


@app.route('/api/printRep/<repNum>')
def printRep(repNum):
	try:
		return json.dumps(getRep(repNum = int(repNum)))
	except Exception as e:
		return json.dumps({"status":'err', 'error_text': str(e)})

@app.route('/api/print_test_flat/<flatNum>')
def printFlatData(flatNum = '219255833'):
	return json.dumps(getFlatData(flatNum = '219255833'))

if __name__ == '__main__':
	app.debug = True
	app.run()