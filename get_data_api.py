import mysql.connector
import json, urllib.request
import codecs
import datetime
from pprint import pprint

conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("select server_name from nodes")
cis=cursor.fetchall();
type(cis)
conn.commit()
cursor.close()

for ci in cis:
	
	reader = codecs.getreader("utf-8")
	
	#print (ci)
	url1 = "https://platform-maintenance.int.thomsonreuters.com/api/v3/jobs?ci=%s&statusNotEquals=dryrun&sort=start:desc&limit=1" %ci
	json_file1 = urllib.request.urlopen(url1)
	data1 = json.load(reader(json_file1))
	type(data1)
	if len(data1) != 0:
		
		ci1 = data1[0]['ci']
		last_patch_status=data1[0]['status']
		last_patch=data1[0]['updated']
		last_patch=last_patch[:-6].replace("T", " ")
		last_patch1 = datetime.datetime.strptime(last_patch, "%Y-%m-%d %H:%M:%S") 
		print("Server Name: ", ci1, "Last Patch Status:",last_patch_status,"Last Patched on: ",last_patch1)
		
		url2 = "https://platform-maintenance.int.thomsonreuters.com/api/v3/ci/%s" %ci
		json_file2 = urllib.request.urlopen(url2)
		data2 = json.load(reader(json_file2))
		type(data2)
		patch_profile=data2['schedule']
		print ("Patch Profile: ", patch_profile)
		
		url3 = "https://platform-maintenance.int.thomsonreuters.com/api/v3/timeslots/schedule/%s" %patch_profile
		json_file3 = urllib.request.urlopen(url3)
		data3 = json.load(reader(json_file3))
		type(data3)
		datetimes=data3['datetimes']
		datetimes=datetimes[0]
		datetimes=datetimes[:-6].replace("T", " ")
		next_patch = datetime.datetime.strptime(datetimes, "%Y-%m-%d %H:%M:%S") 
		print("Next Patch: ", next_patch)
		
		
		conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
		cursor = conn.cursor()
		sel = """select next_patch from patch_details where ci= '%s'""" %ci
		cursor.execute(sel)
		py_val=cursor.fetchone();
		#print(py_val)
		conn.commit()
		cursor.close()
		
		py_val=str(py_val)
		py_yr=py_val[19:py_val.find(",")]
		#print(py_yr)

		py_mnt1=py_val.find(" ")+1
		py_mnt2=py_val[py_mnt1:]
		py_mnt3=py_mnt2.find(",")
		py_mnt=py_mnt2[:py_mnt2.find(",")]
		#print(py_mnt)

		py_dd1=py_mnt2[py_mnt2.find(" ")+1:]
		py_dd=py_dd1[:py_dd1.find(",")]
		#print(py_dd)

		py_hr1=py_dd1[py_dd1.find(" ")+1:]
		py_hr=py_hr1[:py_hr1.find(",")]
		#print(py_hr)

		py_mm1=py_hr1[py_hr1.find(" ")+1:]
		py_mm=py_mm1[:py_mm1.find(",")-1]
		#print(py_mm)
		next_patch=str(next_patch)
		next_patch=next_patch.replace("-","").replace(" ","").replace(":","")
		tbl_yr=next_patch[:4]
		tbl_mnt=next_patch[4:6]
		if (int(tbl_mnt[0]) == 0):
			tbl_mnt=tbl_mnt[1]
			
		tbl_dd=next_patch[6:8]
		if (int(tbl_dd[0]) == 0):
			tbl_dd=tbl_dd[1]
			
		tbl_hr=next_patch[8:10]
		if (int(tbl_hr[0]) == 0):
			tbl_hr=tbl_hr[1]
			
		tbl_mm=next_patch[10:12]
		if (int(tbl_mm[0]) == 0):
			tbl_mm=tbl_mm[1]
			
		if ( int(py_yr) == int(tbl_yr) and int(py_mnt) == int(tbl_mnt) and int(py_dd) == int(tbl_dd) and int(py_hr) == int(tbl_hr) and int(py_mm) == int(tbl_mm)):
			print ("Dates are same")
				
		else:
			conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
			cursor = conn.cursor()
			ins = """INSERT INTO patch_history select * from patch_details where ci= '%s'""" %ci
			cursor.execute(ins)
			print(ci1,"Row moved to history")
			dele = """DELETE FROM patch_details where ci= '%s'""" %ci
			cursor.execute(dele)
			print("Row deleted from main table")
			qur = ("insert into patch_details (ci,last_patch,last_patch_status,patch_profile,next_patch) values (%s,%s,%s,%s,%s)")
			var_val = (ci1,last_patch,last_patch_status,patch_profile,next_patch)
			cursor.execute(qur,var_val)
			conn.commit()
			cursor.close()


conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("drop table display")
print("table dropped")
conn.commit()
cursor.close()

conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("create table display as SELECT * FROM nodes inner JOIN patch_details on nodes.server_name = patch_details.ci")
print("Display table created")
conn.commit()
cursor.close()