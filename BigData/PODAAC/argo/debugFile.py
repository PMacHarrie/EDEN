import requests
import shutil
import netCDF4
from netCDF4 import Dataset
import os

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
    print ('~'.join([netCDF4.chartostring(f['PLATFORM_NUMBER'][:])[0].strip(), str(f['TEMP'][0][i]), f['TEMP_QC'][0][i].decode(), str(f['PRES'][0][i]), f['PRES_QC'][0][i].decode(), str(f['PSAL'][0][i]), f['PSAL_QC'][0][i].decode() ]  )  )


#    os.remove('x.nc')
print (f['PRES'][0][:])
#    print (f['PRES_QC'][0][:])
#    print (f['TEMP'][0][:])
#    print (f['TEMP_QC'][0][:])
#    print (f['PSAL'][0][:])
#    print (f['PSAL_QC'][0][:])
#    print (f['DATA_MODE'][:])
f.close()
