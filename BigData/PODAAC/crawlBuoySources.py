import subprocess
import requests
f = open ('station_table.txt')
station_list = f.readlines()
ctr=1

for s_line in station_list:
    s_line=s_line.rstrip()
    if s_line[0] == '#':
        continue
    f=open('buoy_values.tsv', 'w')
    station_vars=s_line.split('|')
#    print (s_line)
#    print ('Length=', len(station_vars))
    print (station_vars[0], station_vars[6])

    ll=station_vars[6].split()

    obs_lat=-999.0
    obs_lon=-999.0

    if ll[1] == 'S':
       obs_lat = 0.0 - float(ll[0])
    else:
        obs_lat = ll[0]

    if ll[3] == 'W':
        obs_lon = 0.0 - float(ll[2])
    else:
        obs_lon = ll[2] 

    if station_vars[0] <= '32010': 
        continue

    # See if data is avialable for this station
    url='https://www.ndbc.noaa.gov/view_text_file.php'
    good_files=False
    for yr in ['2017', '2018', '2019', '2020', '2021']:
        r = requests.get(url, params={'filename': station_vars[0]+'h' + yr + '.txt.gz', 'dir': 'data/historical/stdmet/'})
        if r.status_code == 200:
            pass
            good_files=True
        else:
#        print ("No data!", r.status_code)
            continue
 
        lines=r.text.split('\n')
        for line in lines:

            if len(line) == 0:
                continue

            x=line.split()
#
#       #YY~MM~DD~hh~mm~WDIR~WSPD~GST~WVHT~DPD~APD~MWD~PRES~ATMP~WTMP~DEWP~VIS~TIDE
#       #yr~mo~dy~hr~mn~degT~m/s~m/s~m~sec~sec~degT~hPa~degC~degC~degC~mi~ft

            #print (station_vars[0], 'line=', x, file=f)
            #print (x[0], x[1], x[2], x[3], x[4])
            obsTime_UTC = x[0] + '-' + x[1] + '-' + x[2] + ' ' + x[3] + ':' + x[4]
            l_out = '~'.join ( [str(obs_lon), str(obs_lat), station_vars[0], obsTime_UTC] +  x[5:17] )
            print (l_out, file=f)
            
    f.close()            
    if good_files:
        subprocess.run(['aws', 's3', 'cp', 'buoy_values.tsv', 's3://nasa-ems-sandbox/staging/noaa_ndbc/' + station_vars[0] + '_buoy_observation.tsv'])

        ctr+=2
#        if ctr >= 1:
#            exit()
