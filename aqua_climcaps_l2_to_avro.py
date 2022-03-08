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


v_debug = False
#v_debug = True

print ('avro start:', datetime.datetime.now().time() )


inputFile = sys.argv[1]
outputFile = sys.argv[2]


d = Dataset(inputFile, 'r')


print (inputFile)
#nj=d['nj']
#ni=d['ni']
#print (d['lat_bnds'][:])
#print (d['fov_lon_bnds'][:])
#print (d['fov_lat_bnds'][:])
#{"name": "fov_lat_bnds", "type": ["float", "null"], "defualt": "NaN"},
#exit()

# Root level '/' is missing from d.groups so add to paths

paths = ['/']

for g in [] + list(d.groups):
#    print (g)
    paths.append('/' + g + '/')

#print (paths)
#exit()

dims = {}
vars = {}
dimgroups = {}

for p in paths:
    if p == '/':
        for dim in d.dimensions.values():
#            print (dim.name, dim.size)
            dims[dim.name] = { 'size': dim.size, 'path': p }
        for v in d.variables.keys():
#            print (d[v].name, d[v].dtype, p, d[v].dimensions)
            vars[d[v].name] = {'dtype' :  str(d[v].dtype), 'path': p, 'fullname' : v, 'dims' : list(d[v].dimensions), 'a' : d[v][:] }
    else:
#        print (p)
        for dim in d[p].dimensions.values():
#            print (dim.name, dim.size)
            dims[dim.name] = { 'size': dim.size, 'path': p }
        for v in d[p].variables.keys():
            #print (v)
#            print (d[p + v].name, d[p+v].dtype, p)
            vars[d[p+v].name] = {'dtype' : str(d[p+v].dtype), 'path': p, 'fullname' : p+v , 'dims' : list(d[p+v].dimensions), 'a': d[p+v][:] }

#print (json.dumps(dims, indent=1))
#print (json.dumps(vars, indent=1))
#print (vars)

for v in sorted(vars):
    dg = str(vars[v]['dims'])
    dg = dg.replace("'", '').replace('[','').replace(']', '').replace(' ', '')
#    print (dg)
    if dg in dimgroups:
        dimgroups[dg].append ( { 'var' : v, 'dims' : vars[v]['dims'] } )
    else:
        dimgroups[dg] = [ { 'var' : v, 'dims' : vars[v]['dims'] } ]
        
    #print (dg, v)

for dg in sorted(dimgroups) : # ['atrack']:
#    print ('*******', dg)
    for v in dimgroups[dg]:
        vn=v['var']
        vj=vars[vn]
        avro_fd = { "name": vn, "type" : ["string" , "null"], "defualt" : "NaN" }
        if vj['dtype'] == 'float32':
            avro_fd['type'][0] = 'float'

        if vj['dtype'] == 'float64':
            avro_fd['type'][0] = 'double'
#        print (json.dumps(avro_fd) + ',')

# avro field descriptor
#        { "name": "a0_cloud", "type": ["string", "null"], "default" : "NaN"},
#

#print ('func_pres:', d['/ave_kern/h2o_vap_func_pres'][:])
#print ('func_indxs:', d['/ave_kern/h2o_vap_func_indxs'][:])
#print ('ave kern:', d['/ave_kern/h2o_vap_ave_kern'])
#print (d['/ave_kern/h2o_vap_ave_kern'][:])

#******* h2o_vap_func_pres
#{"name": "h2o_vap_func_pres", "type": ["float", "null"], "defualt": "NaN"},
#******* h2o_vap_func_pres_bnds
#{"name": "h2o_vap_func_indxs", "type": ["string", "null"], "defualt": "NaN"},


#for a in range(len(d.dimensions['atrack'])):
#    for x in range(len(d.dimensions['xtrack'])):
#        print (a,x,d['/ave_kern/h2o_vap_ave_kern'][a][x][:])
#        for f1 in range(len(d['/ave_kern/h2o_vap_ave_kern'][a][x])):
#            print (a,x,f1,d['/ave_kern/air_temp_func_pres'][f1], d['/ave_kern/h2o_vap_ave_kern'][a][x][f1][:])
#            for f2 in range(len(d['/ave_kern/h2o_vap_ave_kern'][a][x][f1])):
#                print (a,x,f1,f2,d['/ave_kern/air_temp_func_pres'][f1], d['/ave_kern/air_temp_func_pres'][f2], d['/ave_kern/h2o_vap_ave_kern'][a][x][f1][f2])
#exit()


if d.__dict__['AutomaticQualityFlag'] == 'Failed':
    print ('d_name AQF = Failed, skipping...')

#print ('utc time:', d['obs_time_utc'][:])
#exit()

out_line=''

# 3rd dimension setup

v_air_pres = d['air_pres'][:]
v_air_pres_h2o = d['air_pres_h2o'][:]
v_air_pres_lay = d['air_pres_lay'][:]



import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


try:
    schema = avro.schema.parse(open("climcaps_aqua.avsc", "rb").read())
except Exception as exc:
    print (exc)

writer = DataFileWriter(open(outputFile, "wb"), DatumWriter(), schema, codec='deflate')

#reader = DataFileReader(open("users.avro", "rb"), DatumReader())
#for user in reader:
#    print user
#reader.close()

##########################################################################
#
# Output
#
#
# For each atrack/xtract  -->  1 "row"  (Include atrack only variables, xtrack is implied)

#******* atrack
#******* atrack,attitude
#******* atrack,spatial
#******* atrack,xtrack

#    For each 3 Dimensions Grouping (i.e. atrack/xtract,h2o_pres_lay)
#       Array of Records of variables belonging to the dimension group

#******* atrack,xtrack,air_pres -> ['air_temp', 'air_temp_err',  'air_temp_qc', 'co2_vmr', 'co2_vmr_err', 'co2_vmr_qc', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'fg_air_temp', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'gp_hgt', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'gp_hgt_err', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'gp_hgt_qc', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'mw_air_temp', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'mw_air_temp_err', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'mw_air_temp_qc', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'o3_mmr', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'o3_mmr_err', 'dims': ['atrack', 'xtrack', 'air_pres']}, {'var': 'o3_mmr_qc', 'dims': ['atrack', 'xtrack', 'air_pres']}]
#******* atrack,xtrack,air_pres_h2o
#******* atrack,xtrack,air_pres_lay
#******* atrack,xtrack,cld_lay
#******* atrack,xtrack,fov
#******* atrack,xtrack,fov_poly
#******* atrack,xtrack,surf_freq_mw
#******* atrack,xtrack,surf_wnum_ir
#******* atrack,xtrack,utc_tuple

#    4  Dimension groups TBD, pending review.
#******* atrack,xtrack,air_temp_func_pres,air_temp_func_pres
#******* atrack,xtrack,ch4_func_pres,ch4_func_pres
#******* atrack,xtrack,co2_func_pres,co2_func_pres
#******* atrack,xtrack,co_func_pres,co_func_pres
#******* atrack,xtrack,fov,cld_lay
#******* atrack,xtrack,fov,fov_poly
#******* atrack,xtrack,h2o_vap_func_pres,h2o_vap_func_pres
#******* atrack,xtrack,hno3_func_pres,hno3_func_pres
#******* atrack,xtrack,o3_func_pres,o3_func_pres


#******* atrack,xtrack,air_pres_h2o -> [{'var': 'mw_spec_hum', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'mw_spec_hum_err', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'mw_spec_hum_qc', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'rel_hum', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'rel_hum_err', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'rel_hum_qc', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_err', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_qc', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_ice', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_ice_err', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_ice_qc', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_liq', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_liq_err', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}, {'var': 'spec_hum_sat_liq_qc', 'dims': ['atrack', 'xtrack', 'air_pres_h2o']}]
#******* atrack,xtrack,air_pres_lay -> [{'var': 'ch4_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'ch4_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'ch4_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'co_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'co_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'co_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'fg_h2o_vap_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'fg_o3_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_liq_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_liq_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_liq_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_vap_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_vap_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'h2o_vap_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'hno3_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'hno3_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'hno3_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'mw_cld_phase', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'mw_h2o_vap_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'mw_h2o_vap_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'n2o_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'n2o_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'n2o_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'o3_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'o3_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'o3_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'so2_mol_lay', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'so2_mol_lay_err', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}, {'var': 'so2_mol_lay_qc', 'dims': ['atrack', 'xtrack', 'air_pres_lay']}]
#******* atrack,xtrack,air_temp_func_pres,air_temp_func_pres -> [{'var': 'air_temp_ave_kern', 'dims': ['atrack', 'xtrack', 'air_temp_func_pres', 'air_temp_func_pres']}]
#******* atrack,xtrack,ch4_func_pres,ch4_func_pres -> [{'var': 'ch4_ave_kern', 'dims': ['atrack', 'xtrack', 'ch4_func_pres', 'ch4_func_pres']}]
#******* atrack,xtrack,cld_lay -> [{'var': 'for_cld_frac_2lay', 'dims': ['atrack', 'xtrack', 'cld_lay']}, {'var': 'for_cld_frac_2lay_err', 'dims': ['atrack', 'xtrack', 'cld_lay']}, {'var': 'for_cld_frac_2lay_qc', 'dims': ['atrack', 'xtrack', 'cld_lay']}, {'var': 'for_cld_top_pres_2lay', 'dims': ['atrack', 'xtrack', 'cld_lay']}, {'var': 'for_cld_top_pres_2lay_err', 'dims': ['atrack', 'xtrack', 'cld_lay']}, {'var': 'for_cld_top_pres_2lay_qc', 'dims': ['atrack', 'xtrack', 'cld_lay']}]
#******* atrack,xtrack,co2_func_pres,co2_func_pres -> [{'var': 'co2_ave_kern', 'dims': ['atrack', 'xtrack', 'co2_func_pres', 'co2_func_pres']}]
#******* atrack,xtrack,co_func_pres,co_func_pres -> [{'var': 'co_ave_kern', 'dims': ['atrack', 'xtrack', 'co_func_pres', 'co_func_pres']}]
#******* atrack,xtrack,fov -> [{'var': 'blue_spike_fire', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'c2h6_dbt', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'c3h6_dbt', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'c5h8_dbt', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_land_frac', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_lat', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_lon', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_obs_id', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_surf_alt', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_surf_alt_sdev', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'fov_weight', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'ir_precip_est_24hr', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'ir_precip_est_24hr_err', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'ir_precip_est_24hr_qc', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'nh3_dbt', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'num_cld', 'dims': ['atrack', 'xtrack', 'fov']}, {'var': 'num_cld_qc', 'dims': ['atrack', 'xtrack', 'fov']}]
#******* atrack,xtrack,fov,cld_lay -> [{'var': 'cld_frac', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_frac_err', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_frac_qc', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_pres', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_pres_err', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_pres_qc', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_temp', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_temp_err', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}, {'var': 'cld_top_temp_qc', 'dims': ['atrack', 'xtrack', 'fov', 'cld_lay']}]
#******* atrack,xtrack,fov,fov_poly -> [{'var': 'fov_lat_bnds', 'dims': ['atrack', 'xtrack', 'fov', 'fov_poly']}, {'var': 'fov_lon_bnds', 'dims': ['atrack', 'xtrack', 'fov', 'fov_poly']}]
#******* atrack,xtrack,fov_poly -> [{'var': 'lat_bnds', 'dims': ['atrack', 'xtrack', 'fov_poly']}, {'var': 'lon_bnds', 'dims': ['atrack', 'xtrack', 'fov_poly']}]
#******* atrack,xtrack,h2o_vap_func_pres,h2o_vap_func_pres -> [{'var': 'h2o_vap_ave_kern', 'dims': ['atrack', 'xtrack', 'h2o_vap_func_pres', 'h2o_vap_func_pres']}]
#******* atrack,xtrack,hno3_func_pres,hno3_func_pres -> [{'var': 'hno3_ave_kern', 'dims': ['atrack', 'xtrack', 'hno3_func_pres', 'hno3_func_pres']}]
#******* atrack,xtrack,o3_func_pres,o3_func_pres -> [{'var': 'o3_ave_kern', 'dims': ['atrack', 'xtrack', 'o3_func_pres', 'o3_func_pres']}]
#******* atrack,xtrack,surf_freq_mw -> [{'var': 'mw_surf_mw_emis', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}, {'var': 'mw_surf_mw_emis_err', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}, {'var': 'mw_surf_mw_emis_qc', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}, {'var': 'surf_mw_emis', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}, {'var': 'surf_mw_emis_err', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}, {'var': 'surf_mw_emis_qc', 'dims': ['atrack', 'xtrack', 'surf_freq_mw']}]
#******* atrack,xtrack,surf_wnum_ir -> [{'var': 'clim_surf_ir_emis', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'clim_surf_ir_refl', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'clim_surf_ir_wnum', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_emis', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_emis_err', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_emis_qc', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_refl', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_refl_qc', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}, {'var': 'surf_ir_wnum', 'dims': ['atrack', 'xtrack', 'surf_wnum_ir']}]
#******* atrack,xtrack,utc_tuple -> [{'var': 'obs_time_utc', 'dims': ['atrack', 'xtrack', 'utc_tuple']}]


out_j = {}
ctr=0
v_atrack_size = len(d.dimensions['atrack'])
v_xtrack_size = len(d.dimensions['xtrack'])
v_air_pres_h2o_size = len(d.dimensions['air_pres_h2o'])
v_air_pres_size = len(d.dimensions['air_pres'])
v_air_pres_lay_size = len(d.dimensions['air_pres_lay'])
v_cld_lay_size = len(d.dimensions['cld_lay'])
v_fov_size =     len(d.dimensions['fov'])
#v_surf_freq_mw_size = len(d.dimensions['surf_freq_mw'])
v_surf_wnum_ir_size = len(d.dimensions['surf_wnum_ir'])
v_attitude_size = len(d.dimensions['attitude'])
v_spatial_size = len(d.dimensions['spatial'])
v_utc_tuple_size = len(d.dimensions['utc_tuple'])



print ('avro start Main:', datetime.datetime.now().time() )
for a in range(v_atrack_size):
    print ('avro start a:', a, datetime.datetime.now().time() )
    for x in range(v_xtrack_size):
        out_j = {}
        ctr+=1

#        out_j["lon"]        =str(vars['/lon'][a][x])
#        out_j["lat"]        =str(vars['/lat'][a][x])
#        out_j["h2o_liq_tot"]=str(vars['/h2o_liq_tot'][a][x])
#        out_line=''
#        print (ctr, out_j)

    # Handles (atrack) and (atrack,xtrack) variables

        t1=time.time()
        for vn in sorted(vars):  # ( atrack, [xtrack])

            v = vars[vn]['a']

            #vars[d[p+v].name] = {'dtype' : str(d[p+v].dtype), 'path': p, 'fullname' : p+v , 'dims' : list(d[p+v].dimensions), 'a': d[p+v][:] }

            if vars[vn]['dims'] == ['atrack']:  # Handle (atrack) variables
                if vars[vn]['dtype'] == 'float32' or vars[vn]['dtype'] == 'float64':
                    if np.isnan(v[a]):
                        out_j[vn]="NaN"
                    else:
                        out_j[vn]=float(np.format_float_scientific(v[a]))
#                    print ('debug', out_j[vn])
#                    print ('debug', type(out_j[vn]))
                else:
                    out_j[vn]=str(v[a])

        # atrack variables are included with each atrack,xtrack row

            elif vars[vn]['dims'] == ['atrack', 'xtrack']:
                if vars[vn]['dtype'] == 'float32' or vars[vn]['dtype'] == 'float64':
                    if np.isnan(v[a][x]):
                        out_j[vn]="NaN"
                    else:
                        out_j[vn]=float(np.format_float_scientific(v[a][x]))
                else:
                    out_j[vn]=str(v[a][x])
#                    print ('Type:', vars[vn]['dtype'], 'written as string')
            #    print(a,x,v[a][x])
            else:
#                print ('Breaking loop vn')
                continue

#            ctr+=1
#            if ctr == 1:
#                out_line += str(tmp).replace('\n', '').replace(' ', ',') 
#            else:
#                out_line += '~' + str(tmp).replace('\n', '').replace(' ', ',') 

    # Handle other dimensional arrays

        # sat_att	atrack	attitude
        # sat_pos	atrack	spatial
        # sat_vel	atrack	spatial

        out_j['sat_att'] = []
        for l in range(v_attitude_size):
            out_j['sat_att'].append(float(np.format_float_scientific(vars['sat_att']['a'][a][l])))

        out_j['sat_pos'] = []
        for l in range(v_spatial_size):
            out_j['sat_pos'].append(float(np.format_float_scientific(vars['sat_pos']['a'][a][l])))

        out_j['sat_vel'] = []
        for l in range(v_spatial_size):
            out_j['sat_vel'].append(float(np.format_float_scientific(vars['sat_vel']['a'][a][l])))


        out_j['obs_time_utc'] = []
        for l in range(v_utc_tuple_size):
            out_j['obs_time_utc'].append( int(vars['obs_time_utc']['a'][a][x][l]) )

        # Do h20 struct array (atrack,xtrack,air_res_h2o dim variables)
# 
# avro field descriptor for h2o_lay vars
#
#                     "fields":[
#                        {"name":"air_pres_h2o", "type":["float","null"], "default" : "NaN"},
#{"name": "mw_spec_hum", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_spec_hum_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_spec_hum_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "rel_hum", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "rel_hum_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "rel_hum_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "spec_hum", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_ice", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_ice_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_ice_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_liq", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_liq_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "spec_hum_sat_liq_qc", "type": ["string", "null"], "defualt": "NaN"}
#                    ]
        t2=time.time()
        if v_debug:
            print ('a/x vars time', t2-t1)
        t1=time.time()

        h2o=[]
        l_ctr=0
#        for l in range(1):
        for l in range(v_air_pres_h2o_size):
            t=time.time()
            l_ctr+=1
#            print (h_ctr, 'top', t, 0, t-t1)
            h2o.append( {
                'air_pres_h2o'       : float(np.format_float_scientific(vars['air_pres_h2o']['a'][l])),
#                'mw_spec_hum'        : float(np.format_float_scientific(vars['mw_spec_hum']['a'][a][x][l])),
#                'mw_spec_hum_err'    : float(np.format_float_scientific(vars['mw_spec_hum_err']['a'][a][x][l])),
#                'mw_spec_hum_qc'     : str(vars['mw_spec_hum_qc']['a'][a][x][l]),
                'rel_hum'            : float(np.format_float_scientific(vars['rel_hum']['a'][a][x][l])),
                'rel_hum_err'        : float(np.format_float_scientific(vars['rel_hum_err']['a'][a][x][l])),
                'rel_hum_qc'         : str(vars['rel_hum_qc']['a'][a][x][l]),
                'spec_hum'           : float(np.format_float_scientific(vars['spec_hum']['a'][a][x][l])),
                'spec_hum_err'       : float(np.format_float_scientific(vars['spec_hum_err']['a'][a][x][l])),
                'spec_hum_qc'        : str(vars['spec_hum_qc']['a'][a][x][l]),
                'spec_hum_sat_ice'        : float(np.format_float_scientific(vars['spec_hum_sat_ice']['a'][a][x][l])),
                'spec_hum_sat_ice_err'    : float(np.format_float_scientific(vars['spec_hum_sat_ice_err']['a'][a][x][l])),
                'spec_hum_sat_ice_qc'     : str(vars['spec_hum_sat_ice_qc']['a'][a][x][l]),
                'spec_hum_sat_liq'        : float(np.format_float_scientific(vars['spec_hum_sat_liq']['a'][a][x][l])),
                'spec_hum_sat_liq_err'    : float(np.format_float_scientific(vars['spec_hum_sat_liq_err']['a'][a][x][l])),
                'spec_hum_sat_liq_qc'     : str(vars['spec_hum_sat_liq_qc']['a'][a][x][l]),
            }
            )
#            h2o_matrix.append(myStruct)
#            print (h_ctr, 'bottom', time.time(), time.time()-t, time.time()-t1)
#            t1b=time.time()
        
        out_j['h2o_lay'] = h2o
#        t2=time.time()
        t2=time.time()
        if v_debug:
            print (ctr, 'h2o_matrix millis', t2-t1)
#        h2o_matrix=[]






        # Do air pressure matrix struct array (atrack,xtrack,air_pres dim variables)
# avro field descriptor for air_pres vars
#
#{"name": "air_pres", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "air_temp", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "air_temp_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "air_temp_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "co2_vmr", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "co2_vmr_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "co2_vmr_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "fg_air_temp", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "gp_hgt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "gp_hgt_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "gp_hgt_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "mw_air_temp", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_air_temp_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_air_temp_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "o3_mmr", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "o3_mmr_err", "type": ["float", "null"], "defualt": "NaN"}
#{"name": "o3_mmr_qc", "type": ["string", "null"], "defualt": "NaN"}

        t1=time.time()
        ap=[]
#        for l in range(1):
        for l in range(v_air_pres_size):
            ap.append( {
                'air_pres'       : float(np.format_float_scientific(vars['air_pres']['a'][l])),
                'air_temp'       : float(np.format_float_scientific(vars['air_temp']['a'][a][x][l])),
                'air_temp_err'       : float(np.format_float_scientific(vars['air_temp_err']['a'][a][x][l])),
                'air_temp_qc'     : str(vars['air_temp_qc']['a'][a][x][l]),
                'co2_vmr'       : float(np.format_float_scientific(vars['co2_vmr']['a'][a][x][l])),
                'co2_vmr_err'       : float(np.format_float_scientific(vars['co2_vmr_err']['a'][a][x][l])),
                'co2_vmr_qc'     : str(vars['co2_vmr_qc']['a'][a][x][l]),
                'fg_air_temp'       : float(np.format_float_scientific(vars['fg_air_temp']['a'][a][x][l])),
                'gp_hgt'       : float(np.format_float_scientific(vars['gp_hgt']['a'][a][x][l])),
                'gp_hgt_err'       : float(np.format_float_scientific(vars['gp_hgt_err']['a'][a][x][l])),
                'gp_hgt_qc'     : str(vars['gp_hgt_qc']['a'][a][x][l]),
#                'mw_air_temp'       : float(np.format_float_scientific(vars['mw_air_temp']['a'][a][x][l])),
#                'mw_air_temp_err'       : float(np.format_float_scientific(vars['mw_air_temp_err']['a'][a][x][l])),
#                'mw_air_temp_qc'     : str(vars['mw_air_temp_qc']['a'][a][x][l]),
                'o3_mmr'       : float(np.format_float_scientific(vars['o3_mmr']['a'][a][x][l])),
                'o3_mmr_err'       : float(np.format_float_scientific(vars['o3_mmr_err']['a'][a][x][l])),
                'o3_mmr_qc'     : str(vars['o3_mmr_qc']['a'][a][x][l])
                }
            )

        out_j['air_pres_vars'] = ap


        t2=time.time()
        if v_debug:
            print (ctr, 'ap millis', t2-t1)





#
# Do air_pres_lay vars
#
#{"name": "air_pres_lay", "type": ["float", "null"], "defualt": "NaN"},
#******* atrack,xtrack,air_pres_lay
#{"name": "ch4_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "ch4_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "ch4_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "co_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "co_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "co_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "fg_h2o_vap_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fg_o3_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "h2o_liq_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "h2o_liq_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "h2o_liq_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "h2o_vap_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "h2o_vap_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "h2o_vap_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "hno3_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "hno3_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "hno3_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "mw_cld_phase", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "mw_h2o_vap_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_h2o_vap_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "n2o_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "n2o_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "n2o_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "o3_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "o3_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "o3_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "so2_mol_lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "so2_mol_lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "so2_mol_lay_qc", "type": ["string", "null"], "defualt": "NaN"}

        t1=time.time()
        apl=[]
#        for l in range(1):
        for l in range(v_air_pres_lay_size):
            apl.append( {
                'air_pres_lay'       : float(np.format_float_scientific(vars['air_pres_lay']['a'][l])),
                'ch4_mol_lay'        : float(np.format_float_scientific(vars['ch4_mol_lay']['a'][a][x][l])),
                'ch4_mol_lay_err'    : float(np.format_float_scientific(vars['ch4_mol_lay_err']['a'][a][x][l])),
                'ch4_mol_lay_qc'     : str(vars['ch4_mol_lay_qc']['a'][a][x][l]),
                'co_mol_lay'         : float(np.format_float_scientific(vars['co_mol_lay']['a'][a][x][l])),
                'co_mol_lay_err'     : float(np.format_float_scientific(vars['co_mol_lay_err']['a'][a][x][l])),
                'co_mol_lay_qc'      : str(vars['co_mol_lay_qc']['a'][a][x][l]),
                'fg_h2o_vap_mol_lay' : float(np.format_float_scientific(vars['fg_h2o_vap_mol_lay']['a'][a][x][l])),
                'fg_o3_mol_lay'      : float(np.format_float_scientific(vars['fg_o3_mol_lay']['a'][a][x][l])),
#                'h2o_liq_mol_lay'    : float(np.format_float_scientific(vars['h2o_liq_mol_lay']['a'][a][x][l])),
#                'h2o_liq_mol_lay_err' : float(np.format_float_scientific(vars['h2o_liq_mol_lay_err']['a'][a][x][l])),
#                'h2o_liq_mol_lay_qc' : str(vars['h2o_liq_mol_lay_qc']['a'][a][x][l]),
                'h2o_vap_mol_lay'    : float(np.format_float_scientific(vars['h2o_vap_mol_lay']['a'][a][x][l])),
                'h2o_vap_mol_lay_err' : float(np.format_float_scientific(vars['h2o_vap_mol_lay_err']['a'][a][x][l])),
                'h2o_vap_mol_lay_qc' : str(vars['h2o_vap_mol_lay_qc']['a'][a][x][l]),
                'hno3_mol_lay'       : float(np.format_float_scientific(vars['hno3_mol_lay']['a'][a][x][l])),
                'hno3_mol_lay_err'   : float(np.format_float_scientific(vars['hno3_mol_lay_err']['a'][a][x][l])),
                'hno3_mol_lay_qc'    : str(vars['hno3_mol_lay_qc']['a'][a][x][l]),
#                'mw_cld_phase'       : str(vars['mw_cld_phase']['a'][a][x][l]),
#                'mw_h2o_vap_mol_lay' : float(np.format_float_scientific(vars['mw_h2o_vap_mol_lay']['a'][a][x][l])),
#                'mw_h2o_vap_mol_lay_qc' : str(vars['mw_h2o_vap_mol_lay_qc']['a'][a][x][l]),
                'n2o_mol_lay'        : float(np.format_float_scientific(vars['n2o_mol_lay']['a'][a][x][l])),
                'n2o_mol_lay_err'    : float(np.format_float_scientific(vars['n2o_mol_lay_err']['a'][a][x][l])),
                'n2o_mol_lay_qc'     : str(vars['n2o_mol_lay_qc']['a'][a][x][l]),
                'o3_mol_lay'         : float(np.format_float_scientific(vars['o3_mol_lay']['a'][a][x][l])),
                'o3_mol_lay_err'     : float(np.format_float_scientific(vars['o3_mol_lay_err']['a'][a][x][l])),
                'o3_mol_lay_qc'      : str(vars['o3_mol_lay_qc']['a'][a][x][l]),
                'so2_mol_lay'        : float(np.format_float_scientific(vars['so2_mol_lay']['a'][a][x][l])),
                'so2_mol_lay_err'    : float(np.format_float_scientific(vars['so2_mol_lay_err']['a'][a][x][l])),
                'so2_mol_lay_qc'     : str(vars['so2_mol_lay_qc']['a'][a][x][l]),
                }
            )

        out_j['air_pres_lay_vars'] = apl

        t2=time.time()
        if v_debug:
            print (ctr, 'apl millis', t2-t1)



#******* atrack,xtrack,cld_lay
# dim cld_lay	2
#******* cld_lay   
#{"name": "cld_lay_lbl", "type": ["string", "null"], "defualt": "NaN"},         # values are: top, bottom
#{"name": "for_cld_frac_2lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "for_cld_frac_2lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "for_cld_frac_2lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "for_cld_top_pres_2lay", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "for_cld_top_pres_2lay_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "for_cld_top_pres_2lay_qc", "type": ["string", "null"], "defualt": "NaN"},
#
#
        t1=time.time()
        cl=[]
#        for l in range(1):
        for l in range(v_cld_lay_size):
            cl.append( {
                'cld_lay_lbl'               : str(vars['cld_lay_lbl']['a'][l]),
                'for_cld_frac_2lay'         : float(np.format_float_scientific(vars['for_cld_frac_2lay']['a'][a][x][l])),
                'for_cld_frac_2lay_err'     : float(np.format_float_scientific(vars['for_cld_frac_2lay_err']['a'][a][x][l])),
                'for_cld_frac_2lay_qc'      : str(vars['for_cld_frac_2lay_qc']['a'][a][x][l]),
                'for_cld_top_pres_2lay'         : float(np.format_float_scientific(vars['for_cld_top_pres_2lay']['a'][a][x][l])),
                'for_cld_top_pres_2lay_err'     : float(np.format_float_scientific(vars['for_cld_top_pres_2lay_err']['a'][a][x][l])),
                'for_cld_top_pres_2lay_qc'      : str(vars['for_cld_top_pres_2lay_qc']['a'][a][x][l])
                }
            )

        out_j['cld_lay_vars'] = cl
        t2=time.time()
        if v_debug:
            print (ctr, 'cl millis', t2-t1)


#******* atrack,xtrack,fov
# dim fov	9    # dim is implicit in avro fov array
#{"name": "fov", "type": ["string", "null"], "defualt": "0"},
#{"name": "blue_spike_fire", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "c2h6_dbt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "c3h6_dbt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "c5h8_dbt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_land_frac", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_lat", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_lon", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_obs_id", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "fov_surf_alt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_surf_alt_sdev", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_weight", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "ir_precip_est_24hr", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "ir_precip_est_24hr_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "ir_precip_est_24hr_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "nh3_dbt", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "num_cld", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "num_cld_qc", "type": ["string", "null"], "defualt": "NaN"},
#


        t1=time.time()
        fov=[]
#        for l in range(1):
        for l in range(v_fov_size):
            fov.append( {
                'blue_spike_fire'        : float(np.format_float_scientific(vars['blue_spike_fire']['a'][a][x][l])),
                'c2h6_dbt'               : float(np.format_float_scientific(vars['c2h6_dbt']['a'][a][x][l])),
                'c3h6_dbt'               : float(np.format_float_scientific(vars['c3h6_dbt']['a'][a][x][l])),
                'c5h8_dbt'               : float(np.format_float_scientific(vars['c5h8_dbt']['a'][a][x][l])),
                'fov_land_frac'          : float(np.format_float_scientific(vars['fov_land_frac']['a'][a][x][l])),
                'fov_lat'                : float(np.format_float_scientific(vars['fov_lat']['a'][a][x][l])),
                'fov_lon'                : float(np.format_float_scientific(vars['fov_lon']['a'][a][x][l])),
                'fov_obs_id'             : str(vars['fov_obs_id']['a'][a][x][l]),
                'fov_surf_alt'           : float(np.format_float_scientific(vars['fov_surf_alt']['a'][a][x][l])),
                'fov_surf_alt_sdev'      : float(np.format_float_scientific(vars['fov_surf_alt_sdev']['a'][a][x][l])),
                'fov_weight'             : float(np.format_float_scientific(vars['fov_weight']['a'][a][x][l])),
                'ir_precip_est_24hr'     : float(np.format_float_scientific(vars['ir_precip_est_24hr']['a'][a][x][l])),
                'ir_precip_est_24hr_err' : float(np.format_float_scientific(vars['ir_precip_est_24hr_err']['a'][a][x][l])),
                'ir_precip_est_24hr_qc'  : str(vars['ir_precip_est_24hr_qc']['a'][a][x][l]),
                'nh3_dbt'                : float(np.format_float_scientific(vars['nh3_dbt']['a'][a][x][l])),
                'num_cld'                : str(vars['num_cld']['a'][a][x][l]),
                'num_cld_qc'             : str(vars['num_cld_qc']['a'][a][x][l])
                }
            )

        out_j['fov_vars'] = fov
        t2=time.time()
        if v_debug:
            print (ctr, 'fov millis', t2-t1)


# ????
#******* atrack,xtrack,fov,cld_lay
#{"name": "fov", "type": ["string", "null"], "defualt": "0"},
#{"name": "cld_lay_lbl", "type": ["string", "null"], "defualt": "NaN"},         # values are: top, bottom   # dim cld_lay	2
#{"name": "cld_frac", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_frac_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_frac_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "cld_top_pres", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_top_pres_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_top_pres_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "cld_top_temp", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_top_temp_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "cld_top_temp_qc", "type": ["string", "null"], "defualt": "NaN"},


#?  Don't understand how dim_fov 9 and dim fov_poly 8 map to each other, why isn't the center of fov geolocated?
#******* atrack,xtrack,fov_poly
# dim fov	9                                                                             # in this case dim is attribute
# dim fov_poly 8 
#{"name": "lat_bnds", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "lon_bnds", "type": ["float", "null"], "defualt": "NaN"},




#******* atrack,xtrack,surf_freq_mw      ********************* NOT IN AQUA   *******************************
#******* surf_freq_mw
#{"name": "surf_freq_mw", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_surf_mw_emis", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_surf_mw_emis_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "mw_surf_mw_emis_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "surf_mw_emis", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_mw_emis_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_mw_emis_qc", "type": ["string", "null"], "defualt": "NaN"},


#        t1=time.time()
#        sfm=[]
#        for l in range(1):
#        for l in range(v_surf_freq_mw_size):
#            sfm.append( {
#                'mw_surf_mw_emis'     : float(np.format_float_scientific(vars['mw_surf_mw_emis']['a'][a][x][l])),
#                'mw_surf_mw_emis_err' : float(np.format_float_scientific(vars['mw_surf_mw_emis_err']['a'][a][x][l])),
#                'mw_surf_mw_emis_qc'  : str(vars['mw_surf_mw_emis_qc']['a'][a][x][l]),
#                'surf_mw_emis'        : float(np.format_float_scientific(vars['surf_mw_emis']['a'][a][x][l])),
#                'surf_mw_emis_err'    : float(np.format_float_scientific(vars['surf_mw_emis_err']['a'][a][x][l])),
#                'surf_mw_emis_qc'     : str(vars['surf_mw_emis_qc']['a'][a][x][l])
#                }
##            )

#        out_j['sfm_vars'] = sfm
#        t2=time.time()
#        if v_debug:
#            print (ctr, 'sfm millis', t2-t1)




#******* atrack,xtrack,surf_wnum_ir
# dim surf_wnum_ir	100    # becomes row position in array
#{"name": "clim_surf_ir_emis", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "clim_surf_ir_refl", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "clim_surf_ir_wnum", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_ir_emis", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_ir_emis_err", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_ir_emis_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "surf_ir_refl", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "surf_ir_refl_qc", "type": ["string", "null"], "defualt": "NaN"},
#{"name": "surf_ir_wnum", "type": ["float", "null"], "defualt": "NaN"},

        t1=time.time()
        swi=[]
#        for l in range(1):
        for l in range(v_surf_wnum_ir_size):
            swi.append( {
                'clim_surf_ir_emis' : float(np.format_float_scientific(vars['clim_surf_ir_emis']['a'][a][x][l])),
                'clim_surf_ir_emis' : float(np.format_float_scientific(vars['clim_surf_ir_emis']['a'][a][x][l])),
                'clim_surf_ir_wnum' : float(np.format_float_scientific(vars['clim_surf_ir_wnum']['a'][a][x][l])),
                'surf_ir_emis'      : float(np.format_float_scientific(vars['surf_ir_emis']['a'][a][x][l])),
                'surf_ir_emis_err'  : float(np.format_float_scientific(vars['surf_ir_emis_err']['a'][a][x][l])),
                'surf_ir_emis_qc'   : str(vars['surf_ir_emis_qc']['a'][a][x][l]),
                'surf_ir_refl'      : float(np.format_float_scientific(vars['surf_ir_refl']['a'][a][x][l])),
                'surf_ir_refl_qc'   : str(vars['surf_ir_refl_qc']['a'][a][x][l]),
                'surf_ir_wnum'      : float(np.format_float_scientific(vars['surf_ir_wnum']['a'][a][x][l])),
                }
            )

        out_j['swi_vars'] = swi
        t2=time.time()
        if v_debug:
            print (ctr, 'swi millis', t2-t1)




# 4 dim vars

#******* atrack,xtrack,co2_func_pres,co2_func_pres
#{"name": "co2_ave_kern", "type": ["float", "null"], "defualt": "NaN"},
#        co2_kern=[]
#        for r in vars['co2_ave_kern']['a'][a][x]: # rows
#            print(np.format_float_scientific(r[0]))
#            co2_kern.append( { 'co2_r' : np.format_float_scientific(r).tolist() } )
#                'surf_ir_wnum'      : float(np.format_float_scientific(vars['surf_ir_wnum']['a'][a][x][l])),

#        out_j['co2_ave_kern'] = co2_kern
        out_j['co2_ave_kern'] = vars['co2_ave_kern']['a'][a][x].tolist() 

#        print ('co2_kern=', co2_kern)
#        print ('out=', json.dumps(out_j['co2_ave_kern'],indent=1))


#******* atrack,xtrack,co_func_pres,co_func_pres
#{"name": "co_ave_kern", "type": ["float", "null"], "defualt": "NaN"},

        out_j['co_ave_kern'] = vars['co_ave_kern']['a'][a][x].tolist() 



#******* atrack,xtrack,fov,fov_poly
#{"name": "fov_lat_bnds", "type": ["float", "null"], "defualt": "NaN"},
#{"name": "fov_lon_bnds", "type": ["float", "null"], "defualt": "NaN"},
        out_j['fov_lat_bnds'] = vars['fov_lat_bnds']['a'][a][x].tolist() 
        out_j['fov_lon_bnds'] = vars['fov_lon_bnds']['a'][a][x].tolist() 

#******* atrack,xtrack,h2o_vap_func_pres,h2o_vap_func_pres
#{"name": "h2o_vap_ave_kern", "type": ["float", "null"], "defualt": "NaN"},
#        h2o_kern=[]
#        for r in vars['h2o_vap_ave_kern']['a'][a][x]: # rows
#            h2o_kern.append( { 'h2o_r' : r.tolist() } )

        out_j['h2o_vap_ave_kern'] = vars['h2o_vap_ave_kern']['a'][a][x].tolist() 


# 4 dim vars later
#******* atrack,xtrack,hno3_func_pres,hno3_func_pres
#{"name": "hno3_ave_kern", "type": ["float", "null"], "defualt": "NaN"},

        out_j['hno3_ave_kern'] = vars['hno3_ave_kern']['a'][a][x].tolist() 


#******* atrack,xtrack,o3_func_pres,o3_func_pres
#{"name": "o3_ave_kern", "type": ["float", "null"], "defualt": "NaN"},


        out_j['o3_ave_kern'] = vars['o3_ave_kern']['a'][a][x].tolist() 



#        print (json.dumps(out_j, indent=1))

#
# write the avro record
#

        t2=round(time.time() * 1000)
        try:
            writer.append(out_j)
        except Exception as exc:
            print (exc)
        t3=round(time.time() * 1000)
        if v_debug:
            print (ctr, 'writer millis', t3-t2)


# Done close avro file


writer.close()
print ('avro close:', datetime.datetime.now().time() )

#reader = DataFileReader(open("climcaps.avro", "rb"), DatumReader())
#for observation in reader:
#    print (observation)
#reader.close()

print (outputFile)

print ('avro end:', datetime.datetime.now().time() )


exit()
