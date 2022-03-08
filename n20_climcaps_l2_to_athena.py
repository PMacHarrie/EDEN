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


d_name='SNDR.J1.CRIMSS.20210731T0000.m06.g001.L2_CLIMCAPS_RET.std.v02_28.G.210921124740.nc'
d = Dataset(d_name, 'r')
#nj=d['nj']
#ni=d['ni']
#print (d)

#for g in [] + list(d.groups):
#    print (g)
#exit()

#for dimobj in d.dimensions.values():
#    print('/', dimobj.name, dimobj.size)
#for dimobj in d['/mw'].dimensions.values():
#    print('/mw/', dimobj.name, dimobj.size)
#for dimobj in d['/mol_lay'].dimensions.values():
#    print('/mol_lay/', dimobj.name, dimobj.size)
#for dimobj in d['/ave_kern'].dimensions.values():
#    print('/ave_kern/', dimobj.name, dimobj.size)
#for dimobj in d['/aux'].dimensions.values():
#    print('/aux', dimobj.name, dimobj.size)
#for v in d.variables.keys():
#    var = d[v]
#    print (var.group().name +  v, end=',')
#    for dim in var.dimensions:
#        print (dim, end='~')
#    print ()

#d_mw=d['/mw']
#for v in d_mw.variables.keys():
#    var = d_mw[v]
#    print ( '/' + var.group().name + '/' +  v, end=',')
#    for dim in var.dimensions:
#        print (dim, end='~')
#    print ()

#d_mw=d['/mol_lay']
#for v in d_mw.variables.keys():
#    var = d_mw[v]
#    print ( '/' + var.group().name + '/' +  v, end=',')
#    for dim in var.dimensions:
#        print (dim, end='~')
#    print ()

#d_mw=d['/ave_kern']
#for v in d_mw.variables.keys():
#    var = d_mw[v]
#    print ( '/' + var.group().name + '/' +  v, end=',')
#    for dim in var.dimensions:
#        print (dim, end='~')
#    print ()

#d_mw=d['/aux']
#for v in d_mw.variables.keys():
#    var = d_mw[v]
#    print ( '/' + var.group().name + '/' +  v, end=',')
#    for dim in var.dimensions:
#        print (dim, end='~')
#    print ()

#print (d['/cld_lay_lbl'][:])

#exit()


#print (d['/ave_kern/air_temp_func_pres'])
#print (d['/ave_kern/air_temp_func_pres'][:])
#print ('tmpfunci', d['/ave_kern/air_temp_func_indxs'][:])

#print (d['/ave_kern/h2o_vap_ave_kern'])
#for a in range(len(d.dimensions['atrack'])):
#    for x in range(len(d.dimensions['xtrack'])):
#        print (a,x,d['/ave_kern/h2o_vap_ave_kern'][a][x][:])
#        for f1 in range(len(d['/ave_kern/h2o_vap_ave_kern'][a][x])):
#            print (a,x,f1,d['/ave_kern/air_temp_func_pres'][f1], d['/ave_kern/h2o_vap_ave_kern'][a][x][f1][:])
#            for f2 in range(len(d['/ave_kern/h2o_vap_ave_kern'][a][x][f1])):
#                print (a,x,f1,f2,d['/ave_kern/air_temp_func_pres'][f1], d['/ave_kern/air_temp_func_pres'][f2], d['/ave_kern/h2o_vap_ave_kern'][a][x][f1][f2])
#exit()




# Main dimensions (atrack[,xtrack]) variable list

climcaps_varlist = [
"/asc_flag",
"/mean_anom_wrt_equat",
"/sat_alt",
"/sat_sol_azi",
"/sat_sol_zen",
"/scan_mid_time",
"/subsat_lat",
"/subsat_lon",
"/sun_glint_lat",
"/sun_glint_lon",
"/sat_att",
"/sat_pos",
"/sat_vel",
"/air_pres_h2o_nsurf",
"/air_pres_lay_nsurf",
"/air_pres_nsurf",
"/air_temp_dof",
"/aux/a0_cloud",
"/aux/aeff_1",
"/aux/aeff_end",
"/aux/ampl_eta",
"/aux/bad_mw_ret",
"/aux/bad_phys_ret",
"/aux/bad_reg_ret",
"/aux/bt2",
"/aux/chi2_ch4",
"/aux/chi2_co",
"/aux/chi2_co2",
"/aux/chi2_h2o",
"/aux/chi2_hno3",
"/aux/chi2_n2o",
"/aux/chi2_o3",
"/aux/chi2_so2",
"/aux/chi2_temp",
"/aux/cldfrc_500",
"/aux/cldfrc_tot",
"/aux/clim_co2_mmr",
"/aux/clim_surf_ir_wnum_cnt",
"/aux/etarej",
"/aux/fg_surf_air_temp",
"/aux/fg_surf_temp",
"/aux/for_cld_frac_tot",
"/aux/for_cld_frac_tot_err",
"/aux/for_cld_frac_tot_qc",
"/aux/for_cld_top_pres_tot",
"/aux/for_cld_top_pres_tot_err",
"/aux/for_cld_top_pres_tot_qc",
"/aux/idprof",
"/aux/ir_x",
"/aux/ispare_2",
"/aux/nbest",
"/aux/ngood",
"/aux/pbest",
"/aux/pgood",
"/aux/prior_sea_lev_pres",
"/aux/prior_surf_pres",
"/aux/qualsurf",
"/aux/qualtemp",
"/aux/softcode",
"/aux/surf_dew_point_temp",
"/aux/surf_dew_point_temp_qc",
"/aux/surf_h2o_vap_pres_deficit",
"/aux/surf_h2o_vap_pres_deficit_qc",
"/aux/totliqwat",
"/ave_kern/air_temp_func_last_indx",
"/ave_kern/ch4_func_last_indx",
"/ave_kern/co_func_last_indx",
"/ave_kern/co2_func_last_indx",
"/ave_kern/h2o_vap_func_last_indx",
"/ave_kern/hno3_func_last_indx",
"/ave_kern/o3_func_last_indx",
"/ch4_dof",
"/ch4_mmr_midtrop",
"/ch4_mmr_midtrop_err",
"/ch4_mmr_midtrop_qc",
"/co_dof",
"/co_mmr_midtrop",
"/co_mmr_midtrop_err",
"/co_mmr_midtrop_qc",
"/co2_dof",
"/h2o_liq_tot",
"/h2o_liq_tot_err",
"/h2o_liq_tot_qc",
"/h2o_vap_dof",
"/h2o_vap_tot",
"/h2o_vap_tot_err",
"/h2o_vap_tot_qc",
"/hno3_dof",
"/land_frac",
"/lat",
"/lat_geoid",
"/local_solar_time",
"/lon",
"/lon_geoid",
"/mw/mw_h2o_vap_tot",
"/mw/mw_h2o_vap_tot_err",
"/mw/mw_h2o_vap_tot_qc",
"/mw/mw_surf_air_temp",
"/mw/mw_surf_air_temp_err",
"/mw/mw_surf_air_temp_qc",
"/mw/mw_surf_spec_hum",
"/mw/mw_surf_spec_hum_err",
"/mw/mw_surf_spec_hum_qc",
"/mw/mw_surf_temp",
"/mw/mw_surf_temp_err",
"/mw/mw_surf_temp_qc",
"/mw_surf_class",
"/n2o_dof",
"/o3_dof",
"/o3_tot",
"/o3_tot_err",
"/o3_tot_qc",
"/obs_id",
"/obs_time_tai93",
"/sat_azi",
"/sat_range",
"/sat_zen",
"/so2_dof",
"/sol_azi",
"/sol_zen",
"/sun_glint_dist",
"/surf_air_temp",
"/surf_air_temp_err",
"/surf_air_temp_qc",
"/surf_alt",
"/surf_alt_sdev",
"/surf_gp_hgt",
"/surf_gp_hgt_err",
"/surf_gp_hgt_qc",
"/surf_ir_wnum_cnt",
"/surf_rel_hum",
"/surf_rel_hum_err",
"/surf_rel_hum_qc",
"/surf_spec_hum",
"/surf_spec_hum_err",
"/surf_spec_hum_qc",
"/surf_spec_hum_sat_ice",
"/surf_spec_hum_sat_ice_err",
"/surf_spec_hum_sat_ice_qc",
"/surf_spec_hum_sat_liq",
"/surf_spec_hum_sat_liq_err",
"/surf_spec_hum_sat_liq_qc",
"/surf_temp",
"/surf_temp_dof",
"/surf_temp_err",
"/surf_temp_qc",
"/tpause_gp_hgt",
"/tpause_gp_hgt_qc",
"/tpause_pres",
"/tpause_pres_qc",
"/tpause_temp",
"/tpause_temp_qc",
"/view_ang",
]

if d.__dict__['AutomaticQualityFlag'] == 'Failed':
    print ('d_name AQF = Failed, skipping...')

vars = {}

for v in climcaps_varlist:
#    print (v)
#    print (d[v])
    vars[v]=d[v]

#print (vars)

out_line=''
ctr=0
for vn in sorted(vars):
    ctr+=1
    v = vars[vn]
    if ctr == 1:
        out_line += v.name
    else:
        out_line += '~' + v.name

print (out_line)

out_line=''

# 3rd dimension setup

v_air_pres = d['air_pres'][:]
v_air_pres_h2o = d['air_pres_h2o'][:]
v_air_pres_lay = d['air_pres_lay'][:]


# (atrack,xtrack,air_pres_h2o) variables


mw_spec_hum = d['/mw/mw_spec_hum']
mw_spec_hum_err = d['/mw/mw_spec_hum_err']
mw_spec_hum_qc  = d['/mw/mw_spec_hum_qc']
rel_hum = d['rel_hum']
rel_hum_err = d['rel_hum_err']
rel_hum_qc  = d['rel_hum_qc']
spec_hum    = d['spec_hum']
spec_hum_err = d['spec_hum_err']
spec_hum_qc  = d['spec_hum_qc']
spec_hum_sat_ice = d['spec_hum_sat_ice']
spec_hum_sat_ice_err = d['spec_hum_sat_ice_err']
spec_hum_sat_ice_qc = d['spec_hum_sat_ice_qc']
spec_hum_sat_liq = d['spec_hum_sat_liq']
spec_hum_sat_liq_err = d['spec_hum_sat_liq_err']
spec_hum_sat_liq_qc = d['spec_hum_sat_liq_qc']
h2o_liq_mol_lay = d['h2o_liq_mol_lay']
h2o_liq_mol_lay_err = d['h2o_liq_mol_lay_err']
h2o_liq_mol_lay_qc = d['h2o_liq_mol_lay_qc']



# (atrack,xtrack,air_pres_lay) variables
fg_h2o_vap_mol_lay=d['/aux/fg_h2o_vap_mol_lay']
fg_o3_mol_lay     =d['/aux/fg_o3_mol_lay']
h2o_liq_mol_lay=d['/h2o_liq_mol_lay']
h2o_liq_mol_lay_err=d['/h2o_liq_mol_lay_err']
h2o_liq_mol_lay_qc =d['/h2o_liq_mol_lay_qc']
ch4_mol_lay=d['/mol_lay/ch4_mol_lay']
ch4_mol_lay_err = d['/mol_lay/ch4_mol_lay_err']
ch4_mol_lay_qc  = d['/mol_lay/ch4_mol_lay_qc']
co_mol_lay      = d['/mol_lay/co_mol_lay']
co_mol_lay_err  = d['/mol_lay/co_mol_lay_err']
co_mol_lay_qc   = d['/mol_lay/co_mol_lay_qc']
h2o_vap_mol_lay = d['/mol_lay/h2o_vap_mol_lay']
h2o_vap_mol_lay_err = d['/mol_lay/h2o_vap_mol_lay_err']
h2o_vap_mol_lay_qc  = d['/mol_lay/h2o_vap_mol_lay_qc']
hno3_mol_lay        = d['/mol_lay/hno3_mol_lay']
hno3_mol_lay_err    = d['/mol_lay/hno3_mol_lay_err']
hno3_mol_lay_qc     = d['/mol_lay/hno3_mol_lay_qc']
n2o_mol_lay         = d['/mol_lay/n2o_mol_lay']
n2o_mol_lay_err     = d['/mol_lay/n2o_mol_lay_err']
n2o_mol_lay_qc      = d['/mol_lay/n2o_mol_lay_qc']
o3_mol_lay          = d['/mol_lay/o3_mol_lay']
o3_mol_lay_err      = d['/mol_lay/o3_mol_lay_err']
o3_mol_lay_qc       = d['/mol_lay/o3_mol_lay_qc']
so2_mol_lay         = d['/mol_lay/so2_mol_lay']
so2_mol_lay_err     = d['/mol_lay/so2_mol_lay_err']
so2_mol_lay_qc      = d['/mol_lay/so2_mol_lay_qc']
mw_h2o_vap_mol_lay       = d['/mw/mw_h2o_vap_mol_lay']
mw_h2o_vap_mol_lay_qc    = d['/mw/mw_h2o_vap_mol_lay_qc']
mw_cld_phase                = d['/mw_cld_phase']

for a in range(len(d.dimensions['atrack'])):
    for x in range(len(d.dimensions['xtrack'])):
        out_line=''
        ctr=0

    # Handles (atrack) and (atrack,xtrack) variables

        for vn in sorted(vars):  # ( atrack, [xtrack])
            v = vars[vn]
            #print (v.name)
            tmp=v[a]

        # atrack variables are included with each atrack,xtrack row

            if list(v.dimensions) == ['atrack']:
                tmp=v[a]
            #    print(a,x,v[a])
            elif list(v.dimensions) == ['atrack', 'xtrack']:
                tmp=v[a][x]
            #    print(a,x,v[a][x])
            else:
                continue

            ctr+=1
            if ctr == 1:
                out_line += str(tmp).replace('\n', '').replace(' ', ',') 
            else:
                out_line += '~' + str(tmp).replace('\n', '').replace(' ', ',') 

    # Handle other dimensional arrays

        # sat_att	atrack	attitude
        # sat_pos	atrack	spatial
        # sat_vel	atrack	spatial
        out_line += '~' + str(d['sat_att'][a].tolist())
        out_line += '~' + str(d['sat_pos'][a].tolist())
        out_line += '~' + str(d['sat_vel'][a].tolist())
        #print (str(d['sat_pos'][a].tolist()))
        #print (str(d['sat_vel'][a].tolist()))

        # Do h20 struct array (atrack,xtrack,air_res_h2o dim variables)
        h2o_matrix=[]
        for l in range(len(d.dimensions['air_pres_h2o'])):
            myStruct={
                'v_air_pres_h2o' : str(v_air_pres_h2o[l]),
                'rel_hum'        : str(rel_hum[a][x][l]),
                'rel_hum_err'    : str(rel_hum_err[a][x][l]),
                'rel_hum_qc'     : str(rel_hum_qc[a][x][l]),
                'spec_hum'        : str(spec_hum[a][x][l]),
                'spec_hum_err'    : str(spec_hum_err[a][x][l]),
                'spec_hum_qc'     : str(spec_hum_qc[a][x][l]),
                'spec_hum_sat_ice'        : str(spec_hum_sat_ice[a][x][l]),
                'spec_hum_sat_ice_err'    : str(spec_hum_sat_ice_err[a][x][l]),
                'spec_hum_sat_ice_qc'     : str(spec_hum_sat_ice_qc[a][x][l]),
                'spec_hum_sat_liq'        : str(spec_hum_sat_liq[a][x][l]),
                'spec_hum_sat_liq_err'    : str(spec_hum_sat_liq_err[a][x][l]),
                'spec_hum_sat_liq_qc'     : str(spec_hum_sat_liq_qc[a][x][l]),
                'h2o_liq_mol_lay'        : str(h2o_liq_mol_lay[a][x][l]),
                'h2o_liq_mol_lay_err'    : str(h2o_liq_mol_lay_err[a][x][l]),
                'h2o_liq_mol_lay_qc'     : str(h2o_liq_mol_lay_qc[a][x][l]),
            }
            h2o_matrix.append(myStruct)
            
        out_line += '~' + str(json.dumps(h2o_matrix)).replace('\n', '').replace(' ', '')
        h2o_matrix=[]

        # Do air pressure layer matrix struct array (atrack,xtrack,air_pres_lay dim variables)
        apl_matrix=[]
        for l in range(len(d.dimensions['air_pres_lay'])):
            myStruct={
                'v_air_pres_lay' : str(v_air_pres_lay[l]),
                'fg_h2o_vap_mol_lay'        : str(fg_h2o_vap_mol_lay[a][x][l]),
                'fg_o3_mol_lay'    : str(fg_o3_mol_lay[a][x][l]),
                'h2o_liq_mol_lay'     : str(h2o_liq_mol_lay[a][x][l]),
                'h2o_liq_mol_lay_err'     : str(h2o_liq_mol_lay_err[a][x][l]),
                'h2o_liq_mol_lay_qc'     : str(h2o_liq_mol_lay_qc[a][x][l]),
                'ch4_mol_lay'     : str(ch4_mol_lay[a][x][l]),
                'ch4_mol_lay_err'     : str(ch4_mol_lay_err[a][x][l]),
                'ch4_mol_lay_qc'     : str(ch4_mol_lay_qc[a][x][l]),
                'co_mol_lay'     : str(co_mol_lay[a][x][l]),
                'co_mol_lay_err'     : str(co_mol_lay_err[a][x][l]),
                'co_mol_lay_qc'     : str(co_mol_lay_qc[a][x][l]),
                'h2o_vap_mol_lay'     : str(h2o_vap_mol_lay[a][x][l]),
                'h2o_vap_mol_lay_err'     : str(h2o_vap_mol_lay_err[a][x][l]),
                'h2o_vap_mol_lay_qc'     : str(h2o_vap_mol_lay_qc[a][x][l]),
                'hno3_mol_lay'     : str(hno3_mol_lay[a][x][l]),
                'hno3_mol_lay_err'     : str(hno3_mol_lay_err[a][x][l]),
                'hno3_mol_lay_qc'     : str(hno3_mol_lay_qc[a][x][l]),
                'n2o_mol_lay'     : str(n2o_mol_lay[a][x][l]),
                'n2o_mol_lay_err'     : str(n2o_mol_lay_err[a][x][l]),
                'n2o_mol_lay_qc'     : str(n2o_mol_lay_qc[a][x][l]),
                'o3_mol_lay'     : str(o3_mol_lay[a][x][l]),
                'o3_mol_lay_err'     : str(o3_mol_lay_err[a][x][l]),
                'o3_mol_lay_qc'     : str(o3_mol_lay_qc[a][x][l]),
                'so2_mol_lay'     : str(so2_mol_lay[a][x][l]),
                'so2_mol_lay_err'     : str(so2_mol_lay_err[a][x][l]),
                'so2_mol_lay_qc'     : str(so2_mol_lay_qc[a][x][l]),
                'mw_h2o_vap_mol_lay'     : str(mw_h2o_vap_mol_lay[a][x][l]),
                'mw_h2o_vap_mol_lay_qc'     : str(mw_h2o_vap_mol_lay_qc[a][x][l]),
                'mw_h2o_vap_mol_lay'     : str(mw_h2o_vap_mol_lay[a][x][l]),
                'mw_cld_phase'     : str(mw_cld_phase[a][x][l]),
            }
            apl_matrix.append(myStruct)
            
        out_line += '~' + str(json.dumps(apl_matrix)).replace('\n', '').replace(' ', '')
        h2o_matrix=[]


        print (out_line)

exit()

            
        
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

u_list = open('podaac_l2p_urls_v2.txt')
directory="."  
ctr=0

this_batch = []

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

    subprocess.run(["curl", "-O", "-u", "pmacharrie:EwEXyECq0iXKR9zcVZB", "-L", "-n", u])
#    curl -O -u pmacharrie:EwEXyECq0iXKR9zcVZB -L -n https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/GOES16/STAR/v2.70/2021/135/20210515150000-STAR-L2P_GHRSST-SSTsubskin-ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc 

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

#for t in this_batch:
#	print ('deleting t:', t)
#       	rc=subprocess.run(["aws", "s3", "rm", t])
#	print ('rc:',rc)

