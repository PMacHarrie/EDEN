import sys
from netCDF4 import Dataset
import json
import numpy as np
import math
import netCDF4 as nc
from pyathena import connect
import pandas as pd
import json
import time
import datetime
import avro

# Parameters: input and output file names
import sys

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
v_debug=True

try:
    schema = avro.schema.parse(open("test.avsc", "rb").read())
except Exception as exc:
    print (exc)

writer = DataFileWriter(open("na.avro", "wb"), DatumWriter(), schema)

#
out_j=[{"baseRecord":[[1,2],[3,4]],{"baseRecord":[[5,6][7,8]]}]
for i in range(2):
        t2=round(time.time() * 1000)
        try:
            writer.append(out_j[i])
        except Exception as exc:
            print (exc)
        t3=round(time.time() * 1000)
        if v_debug:
            print (ctr, 'writer millis', t3-t2)


# Done close avro file


writer.close()
print ('avro close:', datetime.datetime.now().time() )

reader = DataFileReader(open("na.avro", "rb"), DatumReader())
for observation in reader:
    print (observation)
reader.close()

print (outputFile)

print ('avro end:', datetime.datetime.now().time() )


exit()

            
        
