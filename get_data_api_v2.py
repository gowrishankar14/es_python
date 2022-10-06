import mysql.connector
import json, urllib.request
import codecs
import datetime
import urllib
from pprint import pprint

conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("select server_name from nodes")
cis=cursor.fetchall();
type(cis)
conn.commit()
cursor.close()


for ci in cis:
	try:
		print (ci)
		reader = codecs.getreader("utf-8")
		url1 = "https://api-maintenance-platform.int.thomsonreuters.com/v2/asset/%s" %ci
		json_file1 = urllib.request.urlopen(url1)
		data1 = json.load(reader(json_file1))
		type(data1)
				
		if (len(data1) != 0 and data1['data'] != "null"):
			ci1 = data1['data']['servername']
			last_patch_status=data1['data']['LastRunStatus']
			last_reboot=data1['data']['LastReboot']
			last_reboot=last_reboot[:-6].replace("T", " ")
			last_reboot1 = datetime.datetime.strptime(last_reboot, "%Y-%m-%d %H:%M:%S") 
			patch_profile=data1['data']['PatchProfile']
			next_patch=data1['data']['NextCycleSchedMaint']
			next_patch=next_patch[:-6].replace("T", " ")
			next_patch1 = datetime.datetime.strptime(next_patch, "%Y-%m-%d %H:%M:%S") 
			last_patch=data1['data']['CurrentCycleSchedMaint']
			last_patch=last_patch[:-6].replace("T", " ")
			last_patch1 = datetime.datetime.strptime(last_patch, "%Y-%m-%d %H:%M:%S")
			print (ci1,last_patch_status,last_reboot1,patch_profile,next_patch1,last_patch1)
			
			conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
			cursor = conn.cursor()
			sel = """select next_patch from patch_details_v2 where ci= '%s'""" %ci
			cursor.execute(sel)
			py_val=cursor.fetchone();
			print(py_val)
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
			
			next_patch1=str(next_patch1)
			next_patch1=next_patch1.replace("-","").replace(" ","").replace(":","")
			tbl_yr=next_patch1[:4]
			tbl_mnt=next_patch1[4:6]
			
			if (int(tbl_mnt[0]) == 0):
				tbl_mnt=tbl_mnt[1]
				
			tbl_dd=next_patch1[6:8]
			if (int(tbl_dd[0]) == 0):
				tbl_dd=tbl_dd[1]
				
			tbl_hr=next_patch1[8:10]
			if (int(tbl_hr[0]) == 0):
				tbl_hr=tbl_hr[1]
				
			tbl_mm=next_patch1[10:12]
			if (int(tbl_mm[0]) == 0):
				tbl_mm=tbl_mm[1]
			
			if ( int(py_yr) == int(tbl_yr) and int(py_mnt) == int(tbl_mnt) and int(py_dd) == int(tbl_dd) and int(py_hr) == int(tbl_hr) and int(py_mm) == int(tbl_mm)):
				print ("Dates are same")
		
			else:
				conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
				cursor = conn.cursor()
				ins = """INSERT INTO patch_history_v2 select * from patch_details_v2 where ci= '%s'""" %ci
				cursor.execute(ins)
				print(ci1,"Row moved to history")
				dele = """DELETE FROM patch_details_v2 where ci= '%s'""" %ci
				cursor.execute(dele)
				print("Row delted from main table")
				qur = ("insert into patch_details_v2 (ci,last_patch,last_patch_status,patch_profile,next_patch,last_reboot) values (%s,%s,%s,%s,%s,%s)")
				var_val = (ci1,last_patch1,last_patch_status,patch_profile,next_patch1,last_reboot1)
				print("Row inserted :: ", ci, ci1,last_patch1,last_patch_status,patch_profile,next_patch1,last_reboot1)
				cursor.execute(qur,var_val)
				conn.commit()
				cursor.close()

	except urllib.error.HTTPError as err:
		print(err.code)


conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("drop table display_v2")
print("table dropped")
conn.commit()
cursor.close()
		
conn = mysql.connector.connect( host='localhost',user='root',passwd='Welcome123',db='eds' )
cursor = conn.cursor()
cursor.execute("create table display_v2 as SELECT * FROM nodes inner JOIN patch_details_v2 on nodes.server_name = patch_details_v2.ci")
print("Display table created")
conn.commit()
cursor.close()