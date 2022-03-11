import sys
from netCDF4 import Dataset
import json
import numpy as np
import math
import netCDF4 as nc
import sys
#!{sys.executable} -m pip install PyAthena
from pyathena import connect
#import pyproj
import pandas as pd

d_name='20180130140000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc'
d = Dataset(d_name, 'r')
#nj=d['nj']
#ni=d['ni']
#print (d)
geo_var = d.variables['geostationary']
#print (geo_var)

geo_d = ''

if geo_var.longitude_of_projection_origin == -75.0:
    geo_d = Dataset('G16_075_0_W.nc')
elif geo_var.longitude_of_projection_origin == -75.0:
    geo_d = Dataset('G16_089_5_W.nc')
else:
    print ('Invalid projection_origin. (For this prototype.)')
    print (False)
print (geo_d)

lon=np.around(geo_d['lon'][:],5)
lat=np.around(geo_d['lat'][:],5)

# Debug formula vs. STAR provide geolocation files.

#p = pyproj.Proj(proj='geos', h=geo_var.perspective_point_height, lon_0=geo_var.longitude_of_projection_origin, sweep=geo_var.sweep_angle_axis, datum='WGS84')

#print (p)

#x = d.variables['ni'][:]
#y = d.variables['nj'][:]
    
#X, Y = np.meshgrid(x*geo_var.perspective_point_height, y*geo_var.perspective_point_height)

#lon, lat = p(X, Y, inverse=True)

#for i in range(len(lon)):
#    for j in range(len(lon[i])):
#        if np.isinf(lon[i][j]):
#            continue
        
#        print (i,j, lon[i][j], lat[i][j], geo_d['lon'][i][j], geo_d['lat'][i][j])
#        break
            
        
c_sst_fv = (d['sea_surface_temperature']._FillValue * d['sea_surface_temperature'].scale_factor) + d['sea_surface_temperature'].add_offset


q="""
insert into sci.abi_g16_star_l2p_v2_70
select
  try_cast(sst_dtime as real) sst_dtime, 
  try_cast(longitude as real) longitude, 
  try_cast(latitude as real) latitude, 
  try_cast(satellite_zenith_angle as real) satellite_zenith_angle, 
  try_cast(sea_surface_temperature as real) sea_surface_temperature, 
  try_cast(brightness_temperature_08um6 as real) brightness_temperature_08um6, 
  try_cast(brightness_temperature_10um4 as real) brightness_temperature_10um4, 
  try_cast(brightness_temperature_11um2 as real) brightness_temperature_11um2, 
  try_cast(brightness_temperature_12um3 as real) brightness_temperature_12um3, 
  try_cast(sses_bias as real) sses_bias, 
  try_cast(sses_standard_deviation as real) sses_standard_deviation, 
  try_cast(dt_analysis as real) dt_analysis, 
  try_cast(wind_speed as real) wind_speed, 
  try_cast(sea_ice_fraction as real) sea_ice_fraction, 
  try_cast(l2p_flags as int) l2p_flags, 
  try_cast(quality_level as smallint) quality_level,
  date_parse(time_base, '%Y-%m-%d %H:%i:%s') time_base
from
 sci_staging.abi_g16_star_l2p_staging
order by
 longitude, latitude
"""

import subprocess
import os
import datetime

u_list = open('podaac_l2p_urls.txt')
directory="."  
ctr=0

this_batch = []

for x in u_list:
    u=x.replace("\n","")
    u=x.split('~')[8]
#    print ('x=', x)
#    print ('url=', u)
#    continue
    filename = u.rsplit('/', 1)[-1]
    
    myHour=filename[8:10]

    if True: #myHour in ['17']:           #, '05', '09', '13', '17', '21']:
        #print (filename, myHour)
        pass
    else:
        continue

    print (ctr, 'Getting...', filename, str(datetime.datetime.now()))

# require .netrc in ~
    subprocess.run(["curl", "-O", "-L", "-n", u])
#    curl -O -L -n https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/GOES16/STAR/v2.70/2021/135/20210515150000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc 

    if os.path.isfile(filename):
        pass
    else:
        continue

    inputFileName = filename
    outputFileName = filename.replace('nc', 'csv')
    gzipFileName   = outputFileName + '.gz'
    workPath       = '/home/ec2-user/'                         # more space on root device ???
    s3dest         = 's3://nasa-ems-sandbox/staging/science/g16_abi_l2p/sst/'
    d = Dataset(filename, 'r')
    #d.set_auto_maskandscale(True)
    d.set_auto_mask(False)

    if type(d['sea_surface_temperature'][0]) is np.ma.core.MaskedArray:
        print ('Is masked.')

    sst_dtime= d['sst_dtime'][0]
    sza=np.around(d['satellite_zenith_angle'][0],1)
    sst=np.around(d['sea_surface_temperature'][0],2)
    bt_08um6=np.around(d['brightness_temperature_08um6'][0],3)
    bt_10um4=np.around(d['brightness_temperature_10um4'][0],3)
    bt_11um2=np.around(d['brightness_temperature_11um2'][0],3)
    bt_12um3=np.around(d['brightness_temperature_12um3'][0],3)
    sb=np.around(d['sses_bias'][0],4)
    ssd=np.around(d['sses_standard_deviation'][0],2)
    da=np.around(d['dt_analysis'][0],1)
    ws = np.around(d['wind_speed'][0], 3)
    sif=np.around(d['sea_ice_fraction'][0], 2)
    l2pf=d['l2p_flags'][0]
    ql=d['quality_level'][0]

    time_var = d['time']
    dtime = nc.num2date(time_var[:],time_var.units)
    sst_t_hr = str(dtime[0])
    print (sst_t_hr)

    f=open(workPath + outputFileName, 'w')

#sst_f =  ((9.0/5.0) * (sst - 273)) + 32.0

    y=len(d['sea_surface_temperature'][0][1])
    for i in range(len(d['sea_surface_temperature'][0])):
        for j in range(y):
            if sst[i][j] == c_sst_fv:
                pass
            else:
                csv_line = str(sst_dtime[i][j]) + ',' + str(lon[i][j]) + ',' + str(lat[i][j]) + ','
                csv_line += str(sza[i][j]) + ',' + str( sst[i][j] ) + ',' + str( bt_08um6[i][j] ) + ','
                csv_line += str( bt_10um4[i][j] ) + ',' + str( bt_11um2[i][j] ) + ',' + str( bt_12um3[i][j] ) + ','
                csv_line += str( sb[i][j] ) + ',' + str( ssd[i][j] ) + ',' + str( da[i][j] ) + ','
                csv_line += str(ws[i][j]) + ',' + str(sif[i][j]) + ',' + str( l2pf[i][j] ) + ',' + str( ql[i][j] )
                csv_line += ',' + sst_t_hr
                print (csv_line, file=f )
        #print (i,j)
    f.close()
    os.remove(filename) # delete the nc file
    subprocess.run(["gzip", workPath + outputFileName])
    subprocess.run(["aws", "s3", "cp", workPath + gzipFileName, s3dest + gzipFileName])
    os.remove(workPath + gzipFileName) # delete the gz file
    
    
    #inputFileName = filename
    #outputFileName = filename.replace('nc', 'csv')
    #gzipFileName   = outputFileName + '.gz'
    #workPath       = '/home/ec2-user'                         # more space on root device ???
    #s3dest         = 's3://nasa-ems-sandbox/staging/science/g16_abi_l2p/sst/'
    
    ctr+=1
    this_batch.append(s3dest + gzipFileName)
    
    if ctr >= 8:
        print (datetime.datetime.now(), 'start athena udpate.')
        conn = connect(s3_staging_dir='s3://esdis-ems-athena', region_name='us-west-2')
        pd.options.display.float_format = '{:,.4f}'.format
        df = pd.read_sql(q, conn)
        df
        print (datetime.datetime.now(), 'end athena udpate.')
        for t in this_batch:
            print ('deleting t:', t)
            rc=subprocess.run(["aws", "s3", "rm", t])
            print ('rc:',rc)
        this_batch=[]
        ctr=0
            
#    lon_degree, lat_degree = get_geo(d)
#    print ("done geo", str(datetime.datetime.now()))
#    v_month = get_month(d)


conn = connect(s3_staging_dir='s3://esdis-ems-athena', region_name='us-west-2')
pd.options.display.float_format = '{:,.4f}'.format
df = pd.read_sql(q, conn)
df

for t in this_batch:
    print ('deleting t:', t)
    rc=subprocess.run(["aws", "s3", "rm", t])
    print ('rc:',rc)

