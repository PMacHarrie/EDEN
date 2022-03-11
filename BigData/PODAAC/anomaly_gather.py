import sys
from netCDF4 import Dataset
import json
import numpy as np
import math

# If STAR provided geolocation algorithm is preferred (instead of provided files), uncomment import pyproj.
#import pyproj


import datetime
import pandas as pd

myCFG = {
    "initialize" : False,
    "update"     : True,
    "detect"     : False
}

# Initialize Statistics Dictionary

productLabel='Stats-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0'
monthList = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

if myCFG['initialize']:
    
    for m in monthList:
        print ('Initializing', m)
        stats_dict= {}
        for i in range(-180, 181):
            #print (i, end=':')
            stats_dict[i]={}
            for j in range (-90, 91):
                stats_dict[i][j]= {
                    'm_n'    : str(0),
                    'm_oldM' : str(0.0),
                    'm_newM' : str(0.0),
                    'm_oldS' : str(0.0),
                    'm_newS' : str(0.0)                
                }
        statsFileName= m + '-' + productLabel + '.json'
        f=open(statsFileName, 'w')
        json.dump(stats_dict, f, indent=1)
        f.close()

class RunningStat:
    m_n = 0
    m_oldM =0.0
    m_newM = 0.0
    m_oldS =0.0 
    m_newS = 0.0
    def Clear(self):
        m_n = 0
    def inIT (self,n,oldM,newM,oldS,newS):
        self.m_n=n
        self.m_oldM=oldM
        self.m_newM=newM
        self.m_oldS=oldS
        self.m_newS=newS
    def Push(self,x):
        self.m_n+=1
        if self.m_n == 1:
            self.m_oldM = self.m_newM = x
            self.m_oldS = 0.0
        else:
            self.m_newM = self.m_oldM + ((x - self.m_oldM) / self.m_n)
            self.m_newS = self.m_oldS + ((x - self.m_oldM) * (x - self.m_newM))
            self.m_oldM = self.m_newM
            self.m_oldS = self.m_newS
    def NumDataValues(self):
        return self.m_n
    def Mean(self):
        if self.m_n > 0:
            return self.m_newM
        else:
            return 0.0
    def Variance(self):
        if self.m_n > 1:
            return self.m_newS/(self.m_n -1)
        else:
            return 0.0
    def StandardDeviation(self):
        return math.sqrt(self.Variance())

#d_name='20210514000000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc'
#d = Dataset(d_name, 'r')
#nj=d['nj']
#ni=d['ni']
#print (d)

def get_geo(d):
    geo_var = d.variables['geostationary']

    #print (geo_var)

    geo_d = ''

    if geo_var.longitude_of_projection_origin == -75.0:
        geo_d = Dataset('G16_075_0_W.nc')
    elif geo_var.longitude_of_projection_origin == -75.0:
        geo_d = Dataset('G16_089_5_W.nc')
    else:
        print ('Invalide projection_origin. (For this prototype.)')
        print (False)
    
    #print (geo_d)
    geo_d.set_auto_maskandscale(True)

    lon=geo_d['lon'][:]
    lat=geo_d['lat'][:]

    lon_degree=np.trunc(lon)
    lat_degree=np.trunc(lat)

    #print (lon_degree[~np.isnan(lon_degree)])
    
    return lon_degree, lat_degree

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
            
        

# Get Month of the Year of the observation.
import datetime

def get_month(d):
    v_time = int(d['time'][0])
    #a = datetime.datetime(1980,1,1,0,0,0)
    a = datetime.datetime(1981,1,1,0,0,0)
    b = a + datetime.timedelta(0,v_time) # days, seconds, then other fields.
    v_Month = int(b.strftime('%m'))
    #print ('Month =', v_Month)
    return v_Month

def gather_stats(d, lon, lat, v):

    sst=d[v][0]
    c_sst_fv = (d['sea_surface_temperature']._FillValue * d['sea_surface_temperature'].scale_factor) + d['sea_surface_temperature'].add_offset
    sst_f =  np.around((sst - 273.15) * 9 / 5  + 32.0, 2)
    lon_int = lon.astype(int)
    lat_int = lat.astype(int)
#sza=d['satellite_zenith_angle'][0]
#bt_08um6=d['brightness_temperature_08um6'][0]
#bt_10um4=d['brightness_temperature_10um4'][0]
#bt_11um2=d['brightness_temperature_11um2'][0]
#bt_12um3=d['brightness_temperature_12um3'][0]
#sb=d['sses_bias'][0]
#ssd=d['sses_standard_deviation'][0]
#da=d['dt_analysis'][0]
#ws =d['wind_speed'][0]
#sif=d['sea_ice_fraction'][0]
#l2pf=d['l2p_flags'][0]
#ql=d['quality_level'][0]
    x=len(d['sea_surface_temperature'][0])
    y=len(d['sea_surface_temperature'][0][1])
    for i in range(x):
        for j in range(y):
            if sst[i][j] == c_sst_fv:
                pass
            else:
#                csv_line = str(v_Month) + ',' + str(lon_degree[i][j]) + ',' + str(lat_degree[i][j]) + ',' + str( sst[i][j] )  + ',' + str( sst_f[i][j])
#                lon_int = int(lon_degree[i][j])
#                lat_int = int(lat_degree[i][j])
        
            #print (lon_int, lat_int, csv_line )
            #print (stats_dict[lon_int][lat_int], sst_f_r)
               # my_lon = lon_int[i][j]
               # my_lat = lat_int[i][j]
                if sst_f[i][j] in stats_dict[lon_int[i][j]][lat_int[i][j]]:
                    stats_dict[lon_int[i][j]][lat_int[i][j]][sst_f[i][j]] +=1
                else:
                    stats_dict[lon_int[i][j]][lat_int[i][j]][sst_f[i][j]]=1
        print ("i=", i, str(datetime.datetime.now()))

    #print (i,j)



import os
import subprocess

#stats_dict= {}
#for i in range(-180, 181):
#    print (i, end=':')
#    stats_dict[i]={}
#    for j in range (-90, 91):
#        stats_dict[i][j]= []

u_list = open('podaac_l2p_urls_v2.txt')
directory="."  
ctr=1

for u in u_list:
    u=u.replace("\n","")
    filename = u.rsplit('/', 1)[-1]
    
    myHour=filename[8:10]

    if myHour in ['17']:           #, '05', '09', '13', '17', '21']:
        #print (filename, myHour)
        pass
    else:
        continue

    print (ctr, 'Getting...', filename, str(datetime.datetime.now()))

# requires .netrc in ~
    subprocess.run(["curl", "-O", "-L", "-n", u])
#    curl -O -L -n https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/GOES16/STAR/v2.70/2021/135/20210515150000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc 

    if os.path.isfile(filename):
        pass
    else:
        continue
    
    d = Dataset(filename, 'r')
    d.set_auto_maskandscale(True)
    lon_degree, lat_degree = get_geo(d)
    print ("done geo", str(datetime.datetime.now()))
    v_month = get_month(d)



# Build stats_obj dictionary
    
    myMonth = monthList[v_month-1]
    stats_dict={}
    statsFileName= myMonth + '-' + productLabel + '.json'
    f=open(statsFileName, 'r')
    stats_dict=json.load(f)
    f.close()

    stats_obj={}    
    for lon in stats_dict:
        i=int(lon)
        stats_obj[i]={}
        for lat in stats_dict[lon]:
            j=int(lat)
            n    = int( stats_dict[lon][lat]['m_n'] )
            oldM = float ( stats_dict[lon][lat]['m_oldM'] )
            newM = float ( stats_dict[lon][lat]['m_newM'] )
            oldS = float ( stats_dict[lon][lat]['m_oldS'] )
            newS = float ( stats_dict[lon][lat]['m_newS'] )
            stats_obj[i][j]= RunningStat()
            stats_obj[i][j].inIT(n, oldM, newM, oldS, newS)
            
    
    print ('Month=', v_month, myMonth)
    #print (d)
    sst   =  d['sea_surface_temperature'][0]
    sst_f =  np.around((sst - 273.15) * 9 / 5  + 32.0, 2)
    print ("start gather", str(datetime.datetime.now()))
    #gather_stats(d, lon_degree, lat_degree, 'sea_surface_temperature')
    lon_f=lon_degree.flatten()
    lat_f=lat_degree.flatten()
    sst_flat=sst.flatten()
    x=np.dstack((lon_f, lat_f, sst_flat))
    #print (x[0].shape)
    #print (x[0][0])
    dataset = pd.DataFrame({'lon': x[0][:, 0], 'lat': x[0][:, 1], 'sst': x[0][:, 2]})
    sst_llgb=dataset[dataset['sst']>=-3276.0].groupby(['lon','lat'])
    for row,data in sst_llgb:
    #ctr+=1
    #if ctr <= 10:
        #print ('data w=', data['lon'].values[0], 'x', data['lat'].values[0], 'y', data['sst'].values)
        lon=data['lon'].values[0]
        lat=data['lat'].values[0]
        #stats_dict[lon][lat].extend(data['sst'].values)
        for s in data['sst'].values:
            stats_obj[lon][lat].Push(s)

    print ("done gather", str(datetime.datetime.now()))

# Write stats_dict
    
    stats_dict={}
    for lon in stats_obj:
        stats_dict[lon]={}
        for lat in stats_obj[lon]:
            stats_dict[lon][lat]={
            'm_n'    : str(stats_obj[lon][lat].m_n    ),
            'm_oldM' : str(stats_obj[lon][lat].m_oldM ),
            'm_newM' : str(stats_obj[lon][lat].m_newM ),
            'm_oldS' : str(stats_obj[lon][lat].m_oldS ),
            'm_newS' : str(stats_obj[lon][lat].m_newS )
            }

    f=open(statsFileName, 'w')
    json.dump(stats_dict, f, indent=1)
    f.close()
    #doIContinue=True
    #for i in stats_dict:
    #    for j in stats_dict[i]:
    #        if len(stats_dict[i][j]) > 0:
    #            print (i,j, 'size=', len(stats_dict[i][j]), stats_dict[i][j])
    #            doIContinue=False
    #            break
    #    if doIContinue:
    #        pass
    #    else:
    #        break
                
    os.remove(filename)
    ctr += 1
#    if ctr >= 999999:
#        break

