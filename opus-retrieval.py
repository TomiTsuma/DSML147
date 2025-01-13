
import psycopg2
import sqlalchemy as sq
import pandas as pd
import numpy as np
import mariadb
import dask.dataframe as dd
import json
import os
import shutil
import subprocess
from pathlib import Path
import pyodbc
import sys


username = "scoring"
password = "idkltb93e0eomejp"
host = 'spectral-msql-jul-24-backup-do-user-2276924-0.b.db.ondigitalocean.com'
port = 25060
database="farmlabv3_live"
# schema="farmlabv3_live"
# schema="historical"

scores = pd.read_csv("scoring_output.csv")

def get_db_cursor():
    try:
        conn = mariadb.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
    
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

conn = get_db_cursor()
cur = conn.cursor()

conn_lims = pyodbc.connect("Driver={SQL Server};"
                            "Server=192.168.5.18\CROPNUT;"
                            "Database=cropnuts;"
                            "uid=thomasTsuma;pwd=GR^KX$uRe9#JwLc6")

def classifyResult(X, lim1, lim2, lim3):
    if X <= lim1:
        return "very low"
    elif X <= lim2:
        return "low"
    elif X <= lim3:
        return "optimum"
    else:
        return "high"

while True:
    comparison_df_name = input("Enter the name of the comparison file")
    if(str(comparison_df_name).endswith(".csv")):
        pass
    else:
        comparison_df_name = comparison_df_name+".csv"
    try:
        comparison_df = pd.read_csv(f"inputFiles/{comparison_df_name}")
        break    
    except Exception as e:
        print(e)

while True:
    aez_df_name = input("Enter the name of the aez file")
    if(str(aez_df_name).endswith(".csv")):
        pass
    else:
        aez_df_name = aez_df_name+".csv"
    try:
        aez_df = pd.read_csv(f"inputFiles/{aez_df_name}")
        break    
    except Exception as e:
        print(e)

renamed_opus_folder = input("Name of renamed opus folder: opus_renamed")
opus_folder = input("Name of opus folder: opus")
spectral_sample_output_file = input("Spectral Sample Output file")


config = pd.read_sql("""SELECT [Chemical_Config_Id]
       ,[chemical_code]
      ,[Spectral_Lod]
      ,[spectral_Decimal_places]
      ,[spectral_Significant_figure]
  FROM [cropnuts].[dbo].[Chemicals_Config]
  where [Type_Code]=4""",con=conn_lims)
chemicals = pd.read_sql("SELECT chemical_code, chemical_name FROM Chemicals",con=conn_lims)
config = pd.merge(config, chemicals,on="chemical_code",how="inner")
config.chemical_name = [i.lower().replace(" ","_").replace("(","").replace(")","").replace(".","") for i in config.chemical_name]
config= config[['chemical_name','spectral_Decimal_places']]
config = config.set_index('chemical_name')
config = config.to_dict()

for column in scores.columns:
    if column in config['spectral_Decimal_places'].keys():
        if (config['spectral_Decimal_places'][column]) >= 0:
            scores[column] = scores[column].round(decimals=int(config['spectral_Decimal_places'][column]))

aez_df = aez_df.reset_index()
reports_pivot_2 = pd.read_csv("inputFiles/reports_final.csv")

spectral_sample_df = pd.DataFrame()
conn = get_db_cursor()
cur = conn.cursor()
renaming = {}

scores = scores.drop_duplicates(subset="spectral_sample_id")
scores['organic_matter'] = scores['organic_carbon'] * 1.72

os.makedirs(f"outputFiles/{opus_folder}",exist_ok=True)
os.makedirs(f"outputFiles/{renamed_opus_folder}",exist_ok=True)
count = 0
spectral_sample_df = pd.DataFrame()

for index, row in comparison_df.iterrows():
    barcode = row['barcode'].strip()
    crop = row['crop'].strip()
    client = row['client_name'].strip()
    farm = row['farm_name'].strip()
    ph = str(row['ph']).strip()
    phosphorus = str(row['phosphorus']).strip()
    potassium = str(row['potassium']).strip()
    organic_matter = str(row['Organic Matter']).strip()
    calcium = str(row['calcium']).strip()
    # magnesium = str(row['magnesium']).strip()
    print(barcode)
    # if(barcode != 'TEST-DS1-0023'):
    #     continue

    try:
        aez = row['AEZ_name']
    except Exception as e:
        print(e)

    # folder = f"./outputFiles/{crop}_ph-{ph}_phosphorus-{phosphorus}_potassium-{potassium}_organicmatter-{organic_matter}_calcium-{calcium}_magnesium-{magnesium}"
    folder = f"./outputFiles/{crop.replace(" ","")}_ph-{ph}_phosphorus-{phosphorus}_potassium-{potassium}_organicmatter-{organic_matter}_calcium-{calcium}"
    if(folder in os.listdir("./outputFiles")):
        continue
    os.makedirs(folder, exist_ok=True)
    
    vindexes = pd.read_sql(f"""
        SELECT vIndexes.guide, Clients.client_id, Clients.client_name, LOWER(vIndexes.status_name) AS status_name, Crops.crop_name, vIndexes.crop_code, LOWER(vIndexes.Chemical_Name) AS chemical_name 
        FROM vIndexes 
        INNER JOIN Crops 
        ON Crops.crop_code = vIndexes.crop_code
        INNER JOIN Clients
        ON Clients.client_id = vIndexes.client_id
        WHERE 
        vIndexes.crop_name = '{crop}' AND 
        chemical_name IN ('ph','organic matter','calcium','potassium','magnesium') AND
        Clients.client_name = '{client}' AND
        guide IS NOT NULL
    """,con=conn_lims)
    if len(vindexes) == 0:
        print("No guides found for:")
        print(crop)
        print(client)
        print("*******************************************")
        # continue
        vindexes = pd.read_sql(f"""
        SELECT vIndexes.guide, Clients.client_id, Clients.client_name, LOWER(vIndexes.status_name) AS status_name, Crops.crop_name, vIndexes.crop_code, LOWER(vIndexes.Chemical_Name) AS chemical_name 
        FROM vIndexes 
        INNER JOIN Crops 
        ON Crops.crop_code = vIndexes.crop_code
        INNER JOIN Clients
        ON Clients.client_id = vIndexes.client_id
        WHERE 
        vIndexes.crop_name LIKE '%{crop}%' AND 
        chemical_name IN ('ph','organic matter','calcium','potassium','magnesium') AND
        Clients.client_name LIKE '%Yara Kenya%' AND
        guide IS NOT NULL
        """,con=conn_lims)
    if len(vindexes) == 0:
        print("No guides found for:")
        print(crop)
        print(client)
        print("*******************************************")
        continue
    vindexes.to_csv(f"{folder}/vindexes.csv")
    vindexes.chemical_name =  [str(i).replace(" ","_") for i in vindexes.chemical_name]
    vindexes.chemical_name = [i.strip().replace(" ","_").replace(".","").replace("(","").replace(")","") for i in vindexes.chemical_name]
    vindexes = vindexes.drop_duplicates()
    
    calcium_guides = vindexes.loc[vindexes['chemical_name']=='calcium']
    calcium_guides = calcium_guides.drop_duplicates(subset='status_name')
    calcium_guides = calcium_guides[['status_name','guide']]
    calcium_guides = calcium_guides.set_index('status_name')
    calcium_guides = calcium_guides.to_dict()
    print("Calcium guides:", calcium_guides)
    
    # magnesium_guides = vindexes.loc[vindexes['chemical_name']=='magnesium']
    # magnesium_guides = magnesium_guides.drop_duplicates(subset='status_name')
    # magnesium_guides = magnesium_guides[['status_name','guide']]
    # magnesium_guides = magnesium_guides.set_index('status_name')
    # magnesium_guides = magnesium_guides.to_dict()

    potassium_guides = vindexes.loc[vindexes['chemical_name']=='potassium']
    potassium_guides = potassium_guides.drop_duplicates(subset='status_name')
    potassium_guides = potassium_guides[['status_name','guide']]
    potassium_guides = potassium_guides.set_index('status_name')
    potassium_guides = potassium_guides.to_dict()

    organic_matter_guides = vindexes.loc[vindexes['chemical_name']=='organic_matter']
    organic_matter_guides = organic_matter_guides.drop_duplicates(subset='status_name')
    organic_matter_guides = organic_matter_guides[['status_name','guide']]
    organic_matter_guides = organic_matter_guides.set_index('status_name')
    organic_matter_guides = organic_matter_guides.to_dict()
    print("OM guides:", organic_matter_guides)
    
    ph_guides = vindexes.loc[vindexes['chemical_name']=='ph']
    ph_guides = ph_guides.drop_duplicates(subset='status_name')
    ph_guides = ph_guides[['status_name','guide']]
    ph_guides = ph_guides.set_index('status_name')
    ph_guides = ph_guides.to_dict()
    print("ph guides:", ph_guides)

    crop_reports = scores.copy(deep=True)

    crop_reports['ph_classes'] = crop_reports['ph'].apply(classifyResult, args=(ph_guides['guide']['very low'],ph_guides['guide']['low'],ph_guides['guide']['high']))                                              
    crop_reports['organic_matter_classes'] = crop_reports['organic_matter'].apply(classifyResult, args=(organic_matter_guides['guide']['very low'],organic_matter_guides['guide']['low'],organic_matter_guides['guide']['high']))                                              
    crop_reports['calcium_classes'] = crop_reports['calciumPercSat'].apply(classifyResult, args=(calcium_guides['guide']['very low'],calcium_guides['guide']['low'],calcium_guides['guide']['high']))
    # crop_reports['magnesium'] = crop_reports['magnesiumPercSat'].apply(classifyResult, args=((magnesium_guides['guide']['very low'],magnesium_guides['guide']['low'],magnesium_guides['guide']['critical'])))
    crop_reports['potassium_classes'] = crop_reports['potassiumPercSat'].apply(classifyResult, args=((potassium_guides['guide']['very low'],potassium_guides['guide']['low'],potassium_guides['guide']['critical'])))
    crop_reports['phosphorus_classes'] = crop_reports['phosphorus']
    # crop_reports[['spectral_sample_id','ph','ph_classes','potassium','potassium_classes','phosphorus','phosphorus_classes','organic_matter','organic_matter_classes','calciumPercSat','calcium_classes']].to_csv(f"{folder}/crop_reports_not_subsetted.csv")

    
    if(phosphorus in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['phosphorus_classes'] == phosphorus]
        print("phosphorus class: ",phosphorus)
        print("phosphorus",len(crop_reports))
    if(potassium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['potassium_classes'] == potassium]
        print("potassium class: ",potassium)
        print("potassium",len(crop_reports))
    if(ph in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['ph_classes'] == ph]
        print("ph class: ",ph)
        print("ph",len(crop_reports))
    if(calcium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['calcium_classes'] == calcium]
        print("calcium class: ",calcium)
        print("calcium",len(crop_reports))
    if(organic_matter in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['organic_matter_classes'] == organic_matter]
        print("organic matter class: ",organic_matter)
        print("organic_matter",len(crop_reports))
    # if(magnesium in ['low','very low','optimum','high','very high']):
    #     crop_reports = crop_reports.loc[crop_reports['magnesium'] == magnesium]
    #     print("magnesium",len(crop_reports))

    crop_reports[['spectral_sample_id','ph','ph_classes','potassium','potassium_classes','phosphorus','phosphorus_classes','organic_matter','organic_matter_classes','calciumPercSat','calcium_classes']].to_csv(f"{folder}/crop_reports.csv")
    if(len(crop_reports) == 0):
        continue
    client_id = vindexes.client_id.unique()[0]
    crop_code = vindexes.crop_code.unique()[0]
    
    spectral_df = pd.read_sql(f"""
        SELECT 
        spectral_sample_id, 
        barcode AS 'Barcode', 
        farmer_name AS 'Name of Farmer', 
        phone_number AS 'Phone Number',
        sampler_name AS 'Sampler Name', 
        spectral_batch_id, 
        longitude AS 'Longitude',
        latitude AS 'Latitude',
        sample_date AS 'Sample Date', 
        analysis_type AS 'Analysis Name', 
        tree_population AS 'Tree_Population(Total in Field)',  
        TIMESTAMPDIFF(YEAR,'2024-05-28',date_of_planting) AS 'Tree Age(Years)', 
        field_size AS 	'Field Size (Acre)'
        FROM SpectralSample
        WHERE spectral_sample_id IN {str([j for j in crop_reports['spectral_sample_id'].values]).replace("[","(").replace("]",")")} 
        AND spectral_batch_id > 3355
    """, con=conn)
    # scoring_res = pd.read_sql("
    for index, record in spectral_df.iterrows():
        farmlab_barcode = record['Barcode']
        batch_no = record['spectral_batch_id']
        batch_no = str(batch_no).split('.')[0]
        
        if len([i for i in Path(f'./outputFiles/{renamed_opus_folder}').rglob(f"*{barcode}*")]) == 2:
            break
        try:
            subprocess.run(f"scp -r root@161.35.160.152:/mnt/volume_lon1_01/spc_backup/batch_{batch_no} {folder}")
            print(f"Downloaded batch no {batch_no}")
            
            opus_df = pd.DataFrame({'opus': ["_".join(str(i.name).split("_")[:-2]) for i in Path(folder).rglob("**/*.0")]})
            opus_df = opus_df.sort_values("opus")
            opus_df = opus_df[opus_df.duplicated(subset="opus")]
            opus_name = opus_df['opus'].values[0]
            for file in [c for c in Path(folder).rglob(f"*{farmlab_barcode}*")]:
                renaming[barcode] = farmlab_barcode
                converted_opus = [i for i in Path("outputFiles/opus_renamed").rglob(f"*{barcode}*")]
                print(f"No of converted opus files for {barcode}: {len(converted_opus)}")
                if(len(converted_opus) == 2):
                    break
                directory = file.parent
                name = file.name
                actual_barcode = name.split("_")[0]
                
                shutil.copyfile(file,f"outputFiles/{opus_folder}/{name}")
                os.rename(f"outputFiles/{opus_folder}/{name}",f"outputFiles/{renamed_opus_folder}/{barcode}_{len(converted_opus)}.0")
                # renaming[actual_barcode] = barcode
                    
                record['Previous Crop'] = crop
                record['Next Crop'] = crop
                record['Other Crops'] = crop
                record['Barcode'] = barcode
                record['Report Language'] = 'en'
                record['Analysis Name'] = "Starter Soil Scan (IR)"
                record['Tree Age(Years)'] = row['Age']
                record['Tree_Population(Total in Field)'] = row['Plant density']
                record['Field Size (Acre)'] = row['Acres']

                try:
                    record['Latitude'] = aez_df.loc[aez_df['AEZ_name']==aez]['latitude'].values[0]
                    record['Longitude'] = aez_df.loc[aez_df['AEZ_name']==aez]['longitude'].values[0]
                except Exception as e:
                    record['Latitude'] = aez
                    record['Longitude'] = aez
                _ = pd.DataFrame(record).T
                # if row['Yield Target']:
                #     _ = _[['Barcode','Name of Farmer','Phone Number','Sampler Name','Longitude','Latitude','Sample Date','Previous Crop','Next Crop','Other Crops',	'Report Language',	'Analysis Name']]
                # else:
                _ = _[['Barcode','Name of Farmer','Phone Number','Sampler Name','Longitude','Latitude','Sample Date','Previous Crop','Next Crop','Other Crops',	'Report Language',	'Analysis Name', 'Tree_Population(Total in Field)',	'Tree Age(Years)','Field Size (Acre)'	]]
                spectral_sample_df = pd.concat([spectral_sample_df, _])
                spectral_df.to_csv("Spectral Sample Output.csv")
            
        except Exception as e:
            print(e)


with open('renaming.json', 'w') as outfile:
    json.dump(renaming, outfile)
subprocess.run(f"powershell Compress-Archive {os.getcwd()}\\outputFiles\\{renamed_opus_folder} {os.getcwd()}\\outputFiles\\opus.zip")
spectral_sample_df = spectral_sample_df.drop_duplicates(subset="Barcode")
spectral_sample_df.to_excel("outputFiles/Spectral Sample Output.xlsx")