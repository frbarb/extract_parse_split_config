#!/usr/bin/python

import sys
import csv
import re
import os
import glob
import time
from urllib.request import HTTPBasicAuthHandler
import pandas as pd
import zipfile
import datetime
import shutil
import requests
import json


# green flag
genstart = time.time()

# prepare environment
def env_setup():
    envstart = time.time()
    today = datetime.datetime.now()
    date = today.strftime('%Y-%m-%d_%H%M')
    if not os.path.exists(date):
        os.mkdir(date)

    print("\nStartTime")
    print(today)

    global cwd
    cwd = os.getcwd()
    print("\nCurrent working directory is\n" + cwd)
    
    global log
    log = (cwd + "\\" + date + "\\xpt_cfg\\log\\")
    if not os.path.exists(log):
        os.makedirs(log)
    
    global logfile
    logfile = (log + "logfile.txt")
    sys.stdout = open(logfile, 'w')
    
    global carga
    carga = (cwd + "\\" + date + "\\xpt_cfg\\carga\\")
    if not os.path.exists(carga):
        os.makedirs(carga)
    
    global base
    base = (cwd + "\\" + date + "\\xpt_cfg\\base\\")
    if not os.path.exists(base):
        os.makedirs(base)
    
    global _zip
    _zip = (cwd + "\\" + date + "\\xpt_cfg\\zip\\")
    if not os.path.exists(_zip):
        os.makedirs(_zip)        
    
    global xpt_cfg_folder
    xpt_cfg_folder = (cwd + "\\" + date + "\\xpt_cfg\\")      
    
    global lib_folder
    lib_folder = (cwd + '\\repo_files\\lib')  
    print('\nrepository folder for lib program files ' + lib_folder)
    
    # claro repository - to copy all base zip files
    global repo_zip_folder 
    repo_zip_folder = (cwd + '\\repo_files\\zip\\')
    print('\nrepository folder for zipfiles ' + repo_zip_folder)
    
    # make a copy for safety, leaving original claro repo zip files untouched
    repo_zip_list = glob.glob(repo_zip_folder + "*.zip")
    for repozipfile in repo_zip_list:        
        shutil.copy(repozipfile, _zip)

    # copy api script, xpt ip list to YYYY-MM-DD_HHMM \ xpt_cfg folder 
    # api_script = (lib_folder + "\\" + "xpt_api_cfg.ps1")
    # shutil.copy(api_script, xpt_cfg_folder)
    
    # servers_list = (lib_folder + "\\" + "xpt_bf_2_ip.list")    
    # shutil.copy(servers_list, xpt_cfg_folder)
    
    global req_cols
    req_cols = (['cmMac','cmtsStreamType','cmtsStreamAlias','cmtsName','cmStatus','TOP_COD_NODE','COD_NODE','COD_IMOVEL','NUM_CONTRATO','NM_TIPO_EQUIPAMENTO','NOM_LOGR_COMPLETO','NUM_ENDERECO','COD_TIPO_COMPL1','TXT_TIPO_COMPL1','COD_TIPO_COMPL2','TXT_TIPO_COMPL2','COD_TIPO_COMPL','TXT_COMPL','geoLat','geoLng','NOM_BAIRRO'])
    global dtype
    dtype={'cmMac': str,'cmtsName': str,'cmtsStreamType': str,'cmtsStreamAlias': str,'cmStatus': str,'TOP_COD_NODE': str,'COD_NODE': str,'COD_IMOVEL': str,'NUM_CONTRATO': str,'NM_TIPO_EQUIPAMENTO': str,'NOM_LOGR_COMPLETO': str,'NUM_ENDERECO': str,'COD_TIPO_COMPL1': str,'TXT_TIPO_COMPL1': str,'COD_TIPO_COMPL2': str,'TXT_TIPO_COMPL2': str,'COD_TIPO_COMPL': str,'TXT_COMPL': str,'NOM_BAIRRO': str,'geoLat': str,'geoLng': str}
       
    print("\nTempo total preparando env:")
    print("%s seconds" % (time.time() - envstart))

# extract zip files
def extract():
    zipfileslist = glob.glob(_zip + "*.zip")    
    
    for zipfilename in zipfileslist:
        with zipfile.ZipFile(zipfilename, 'r') as zip_ref:
            zipstart = time.time()
            zip_ref.extractall(base)
            print("\nTempo gasto para descompactar: " + zipfilename)
            print("%s seconds" % (time.time() - zipstart))

# parse base files
def parse():
    csvfileslist = glob.glob(base + "*.csv")    
    
    for csvfile in csvfileslist:
        csvstart = time.time()
        #filename = glob.glob('D://VIAVI//scripts//xpertrack_parse_script//python//2022-03-24_1814//xpt_cfg//base//' + "*MCZ*.csv")[0]
        #req_cols = ['cmMac' , 'cmtsName' , 'cmtsStreamType' , 'cmtsStreamAlias' , 'cmStatus' , 'TOP_COD_NODE' , 'COD_NODE' , 'COD_IMOVEL' , 'NUM_CONTRATO' , 'NM_TIPO_EQUIPAMENTO' , 'NOM_LOGR_COMPLETO' , 'NUM_ENDERECO' , 'COD_TIPO_COMPL1' , 'TXT_TIPO_COMPL1' , 'COD_TIPO_COMPL2' , 'TXT_TIPO_COMPL2' , 'COD_TIPO_COMPL' , 'TXT_COMPL' , 'NOM_BAIRRO' , 'geoLat' , 'geoLng']
        #dtype={'cmMac': str,'cmtsName': str,'cmtsStreamType': str,'cmtsStreamAlias': str,'cmStatus': str,'TOP_COD_NODE': str,'COD_NODE': str,'COD_IMOVEL': str,'NUM_CONTRATO': str,'NM_TIPO_EQUIPAMENTO': str,'NOM_LOGR_COMPLETO': str,'NUM_ENDERECO': str,'COD_TIPO_COMPL1': str,'TXT_TIPO_COMPL1': str,'COD_TIPO_COMPL2': str,'TXT_TIPO_COMPL2': str,'COD_TIPO_COMPL': str,'TXT_COMPL': str,'NOM_BAIRRO': str,'geoLat': str,'geoLng': str}
        df = pd.read_csv(csvfile, sep=";", usecols=req_cols, low_memory=True, dtype=dtype, decimal=",")
        #print(df)
        #df = df[df['geoLat'].notna()]
        #df = df[df['geoLng'].notna()]
        df = df[df['cmtsStreamAlias'].notna()]
        #print("filter alias notna")
        #print(df)
        df = df[df['geoLat'].str.startswith('-') & df['geoLng'].str.startswith('-')]
        #print("filter geolat digits comma")
        #print(df)
        df = df[df['cmMac'].str.contains('[a-zA-Z0-9]{12}') & df['cmtsStreamType'].str.contains("UP") & df['cmStatus'].str.contains("operational") & df['NM_TIPO_EQUIPAMENTO'].str.contains("EMTA")]
        #print("filter cmac stream etc")
        #print(df)
        df['geoLat'] = df['geoLat'].str.replace(',', '.')
        #print("replace geolat comma dot")
        #print(df)
        df['geoLng'] = df['geoLng'].str.replace(',', '.')
        #print("replace geolng comma dot")
        #print(df)
        df = df.sort_values(['cmMac']).drop_duplicates(subset = ['cmMac'])
        #print("drop dup cmac")
        #print(df)
        df['cmtsStreamAlias'] = df['cmtsStreamAlias'].str.split('#').str[0]
        #print("split alias")
        #print(df)
        df["Endereço"] = df["NOM_LOGR_COMPLETO"].fillna('') + ' ' + df["NUM_ENDERECO"].fillna('') + ' ' + df["COD_TIPO_COMPL1"].fillna('') + ' ' + df["TXT_TIPO_COMPL1"].fillna('') + ' ' + df["COD_TIPO_COMPL2"].fillna('') + ' ' + df["TXT_TIPO_COMPL2"].fillna('') + ' ' + df["COD_TIPO_COMPL"].fillna('') + ' ' + df["TXT_COMPL"].fillna('')
        df["Concat_1"] = df["NOM_LOGR_COMPLETO"].fillna('') + ' ' +  df["NUM_ENDERECO"].fillna('')
        df["Concat_2"] = df["COD_TIPO_COMPL1"].fillna('') + ' ' + df["TXT_TIPO_COMPL1"].fillna('')
        df["Concat_3"] = df["COD_TIPO_COMPL2"].fillna('') + ' ' + df["TXT_TIPO_COMPL2"].fillna('')
        df["Concat_4"] = df["COD_TIPO_COMPL"].fillna('') + ' ' + df["TXT_COMPL"].fillna('')
        out_file = (carga + "output_xpertrack_cfg_" + os.path.split(csvfile)[1])
        df.to_csv(out_file, sep=';', encoding='utf-8', index=False)   
        print(df.head(5).to_string())
        print('Total de Linhas')
        print(len(df.index))
        print('geoLat Em branco / Blank')
        print(df['geoLat'].isna().sum())
        print('geoLng Em branco / Blank')
        print(df['geoLng'].isna().sum())
        print('cmtsStreamAlias Em branco / Blank')
        print(df['cmtsStreamAlias'].isna().sum())
        print('Em branco / Blank')
        print(df['cmtsStreamAlias'].isna().sum() + df['geoLng'].isna().sum())

# split spo and rjo files
def split():
        
    try:
        spo_df_start = time.time()
        spo_file = glob.glob(carga + "//"+ "*" + "SPO" + "*" + "*.csv")[0]
        df_spo = pd.read_csv(spo_file, sep=";", usecols=req_cols, low_memory=True, dtype=dtype, decimal=",")
        print("\nSPO dataframe creation time taken: ")
        print("%s seconds" % (time.time() - spo_df_start))       
    except IndexError:
        print("\nCan't create spo df. SPO file does not exist")
    
    try:
        rjo_df_start = time.time()
        rjo_file = glob.glob(carga + "//" + "*" + "RJO" + "*" + "*.csv")[0]
        df_rjo = pd.read_csv(rjo_file, sep=";", usecols=req_cols, low_memory=True, dtype=dtype, decimal=",")
        print("\nRJO dataframe creation time taken: ")
        print("%s seconds" % (time.time() - rjo_df_start))
    except IndexError:
        print("\nCan't create rjo df. RJO file does not exist")

    server_ip_dict = {
        'SPO_1': 'xxx.xxx.xxx.xxx',
        'SPO_2': 'xxx.xxx.xxx.xxx',
        'SPO_3': 'xxx.xxx.xxx.xxx',
        'RJO_1': 'xxx.xxx.xxx.xxx',
        'RJO_2': 'xxx.xxx.xxx.xxx',
    }
    
    cmts_list_from_api = {}
    for key, value in server_ip_dict.items():
        api_collect_start = time.time()
        url = 'http://' + value + '/pathtrak/api/cmts'
        r_API = requests.get(url, auth=('yyy', 'xxx'))    
        r_json = r_API.json()                

        cmtslist = []
        for cmts in r_json:
            cmtslist.append(cmts['name'])
        cmtslist_str = "|".join(cmtslist)    
        cmts_list_from_api[key.format(key)] = cmtslist_str        
        print("\n")
        print(cmtslist)
        print("\n" + key + " API Collection time taken: ")
        print("%s seconds" % (time.time() - api_collect_start))
        
    
    # split files respecting cmts list from cmts api
    for key, list in cmts_list_from_api.items():             
        split_start = time.time()

        if "SPO" in key:
            try:
                df_ = df_spo[df_spo['cmtsName'].str.contains(list, case=True, regex=True)]
                out_file = (carga + "_" + key + "_" + os.path.split(spo_file)[1])
                df_.to_csv(out_file, sep=';', encoding='utf-8', index=False)
                print("\n" + key + " creation time taken: ")
                print("%s seconds" % (time.time() - split_start))       
            except UnboundLocalError:
                print("\nSPO dataframe does not exist")
        elif "RJO" in key:
            try:
                df_ = df_rjo[df_rjo['cmtsName'].str.contains(list, case=True, regex=True)]
                out_file = (carga + "_" + key + "_" + os.path.split(rjo_file)[1])
                df_.to_csv(out_file, sep=';', encoding='utf-8', index=False)       
                print("\n" + key + " creation time taken: ")
                print("%s seconds" % (time.time() - split_start))       
            except UnboundLocalError:
                print("\nRJO dataframe does not exist")

# configure servers via api
def conf():
    confstart = time.time()
    real_fileipdict = {
        'CITY': 'xxx.xxx.xxx.xxx',         
        }        
    print("\ncarga folder path: ")
    print(carga)    
    file_list = glob.glob(carga + "//" + "*.csv")
    print("\nFile to be configured: ")
    print(file_list)
    
    for key, ip in real_fileipdict.items():
        for _file in file_list:
            if key in _file:
                print(key, ip, _file)
                import_topology_url = "http://" + ip + "/api/topology/import/csv"
                print(import_topology_url)
                import_topology_header = {'accept': 'application/json'}
                print(import_topology_header)

                _files = {"file":(_file, open(_file,"rb"), 'csv')}
                print(_files)

                login_payload = {'username': 'YYY', 'password': 'XXX'}
                mappings = {'address': 22, 'customField1': 19, 'customField2': 23, 'customField3': 24, 'customField4': 25, 'customField5': 26, 'customField6': 27, 'latitude': 20, 'longitude': 21, 'name': 1, 'nodeName': 4, 'customerId': 9, 'separator': ';'}
                print(mappings)

                try:
                    r_import_topology = requests.post(
                        import_topology_url, 
                        files=_files, 
                        headers=import_topology_header, 
                        params=mappings, 
                        data=login_payload
                        )                 
                    
                    print("\nTempo gasto neste billing topology:")
                    print("%s seconds" % (time.time() - confstart))
                    print(".... \n")
                    print(r_import_topology)
                    print(r_import_topology.url)
                    print(r_import_topology.status_code)
                    print(r_import_topology.headers)
                    print(r_import_topology.text)

                except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout, requests.exceptions.RequestException, requests.exceptions.RetryError) as e:
                    print(e)
                    continue                  

# total time taken
def total_time():
    print("\nTempo total gasto:")
    print("%s seconds" % (time.time() - genstart))
    print("Finished!\n")

# main func
def main():
    env_setup()
    extract()
    parse()
    split()   
    conf() 
    total_time()

# call 2x safe lock
if __name__ == '__main__':
    main()