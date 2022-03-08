import requests
import shutil
import netCDF4
from netCDF4 import Dataset
import os

f = open ('argo_obs_2020.txt')
station_list = f.readlines()
ctr=1

for s_line in station_list:
    s_line=s_line.rstrip()
    if s_line[0] == '#':
        continue
    row = s_line.split(',')
#    print (row[0], row[1], row[2], row[3])
#    print (s_line)

    # See if data is avialable for this station
    url='https://usgodae.org/ftp/outgoing/argo/dac/' + row[0]

#    print ('url=', url)

    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open('x.nc', 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)     
            if r.status_code == 200:
                pass
                good_files=True
            else:
                print ("No data!", r.status_code)
                continue
    else:
        print (r)
        continue

    f = Dataset("x.nc", "r", format="NETCDF")
    f.set_auto_mask(False)
    #print (f)
    #for d in f.dimensions.values():
    #    print (d)
    #for v in f.variables.values():
    #    print (v.name, v.shape, v.datatype)


    for i in range(len(f['PRES'][0][:])):
        if f['TEMP'][0][i] == 99999.0:
            continue
        print ('~'.join([netCDF4.chartostring(f['PLATFORM_NUMBER'][:])[0].strip(), row[1], row[2], row[3], str(f['TEMP'][0][i]), f['TEMP_QC'][0][i].decode(), str(f['PRES'][0][i]), f['PRES_QC'][0][i].decode(), str(f['PSAL'][0][i]), f['PSAL_QC'][0][i].decode() ]  )  )


    f.close()
    os.remove('x.nc')
#    print (f['PRES'][0][:])
#    print (f['PRES_QC'][0][:])
#    print (f['TEMP'][0][:])
#    print (f['TEMP_QC'][0][:])
#    print (f['PSAL'][0][:])
#    print (f['PSAL_QC'][0][:])
#    print (f['DATA_MODE'][:])
