import mysql.connector
import codecs
import datetime
import time
import math
from pprint import pprint
import xml.etree.ElementTree as ET
import urllib
import json, urllib.request
import xmltodict
import socket

conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("select server_name from assets_details where support_group like '%APP-PLATFORM-OPS-EDS%'")
cis=cursor.fetchall();
type(cis)
conn.commit()
cursor.close()

for ci in cis:
	try:
		reader = codecs.getreader("utf-8")
		print (ci)

		url2 = "https://pm.int.thomsonreuters.com/api/v3/ci/%s/detail" %ci
		json_file2 = urllib.request.urlopen(url2)
		data2 = json.load(reader(json_file2))
		type(data2)
				
		if len(data2) != 0:
			active = data2['active']
			blocked = data2['blocked']
			created = data2['created']
			created=datetime.datetime.strptime(created[:19], "%Y-%m-%dT%H:%M:%S")
			inprogress = data2['inprogress']
			lastmaintrun = data2['lastmaintrun']
			lastmaintrun = datetime.datetime.strptime(lastmaintrun[:19], "%Y-%m-%dT%H:%M:%S")
			lastrunstatus = data2['lastrunstatus']
			name = data2['name']
			nextschedrun = data2['nextschedrun']
			nextschedrun = datetime.datetime.strptime(nextschedrun[:19], "%Y-%m-%dT%H:%M:%S")
			schedule = data2['schedule']

		url1 = "https://platform-maintenance.int.thomsonreuters.com/api/v3/jobs?ci=%s&statusNotEquals=dryrun&sort=start:desc&limit=1" %ci
		json_file1 = urllib.request.urlopen(url1)
		data1 = json.load(reader(json_file1))
		type(data1)

		if len(data1) != 0:
			cycle = data1[0]['cycle']
			start = data1[0]['created']
			start = datetime.datetime.strptime(start[:19], "%Y-%m-%dT%H:%M:%S")
			end = data1[0]['end']
			end = datetime.datetime.strptime(end[:19], "%Y-%m-%dT%H:%M:%S")

		print(active, blocked, created, inprogress, lastmaintrun, lastrunstatus, name, nextschedrun, schedule, cycle, start, end)
                                   
    except urllib.error.URLError as e:
		print(e.code)