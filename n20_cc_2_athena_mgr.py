import subprocess
import os
import datetime
from pyathena import connect
import pandas as pd

u_list = open('cc_j1_202108_url.txt')
directory="."
ctr=0

this_batch = []

q="""
insert into sci.climcaps
select
 *,
 cast('n20' as varchar) sat_name_part,
 cast(cast (date_parse(
    cast(obs_time_utc[1] as varchar) || '-' ||
    cast(obs_time_utc[2] as varchar) || '-' || 
    cast(obs_time_utc[3] as varchar),
    '%Y-%c-%e'
 ) as date) as varchar) obs_date_part
 
from 
 sci_staging.climcaps_n20
order by lon
"""


for u in u_list:
    ctr+=1
    u=u.replace("\n","")
    filename = u.rsplit('/', 1)[-1]
    print (u, filename)
    print (ctr, 'Getting...', filename, str(datetime.datetime.now()))
    subprocess.run(["curl", "-n", "-c", ".urs_cookies", "-b", ".urs_cookies", "-LJO", "--url", u])
# curl -n -c .urs_cookies -b .urs_cookies -LJO --url 
#    curl -O -u pmacharrie:EwEXyECq0iXKR9zcVZB -L -n https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/GOES16/STAR/v2.70/2021/135/20210515150000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc 

    if os.path.isfile(filename):
        pass
    else:
        continue

    inputFileName = filename
    outputFileName = filename.replace('nc', 'avro')
#    gzipFileName   = outputFileName + '.gz'
    workPath       = '/home/ec2-user/'                         # more space on root device ???
    s3dest         = 's3://nasa-ems-sandbox/staging/science/climcaps/n20/'
#    d = Dataset(filename, 'r')
#    #d.set_auto_maskandscale(True)
    print (inputFileName, outputFileName, workPath, s3dest)
    subprocess.run(["python", "n20_climcaps_l2_to_avro.py", inputFileName, outputFileName])
    subprocess.run(["aws", "s3", "cp", outputFileName, s3dest + outputFileName])
    os.remove(inputFileName)  # delete the input file
    os.remove(outputFileName) # delete the output file
    this_batch.append(s3dest +outputFileName)
    
    if ctr >= 240:
        print ('Done first t2o.')
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
