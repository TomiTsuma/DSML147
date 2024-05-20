
import pandas as pd
import numpy as np
import mariadb
import os
import shutil
import subprocess
from pathlib import Path
import mariadb
import sys
import json

username = "scoring"
password = "idkltb93e0eomejp"
host = 'spectral-msql-jul-24-backup-do-user-2276924-0.b.db.ondigitalocean.com'
port = 25060
database="farmlabv3_live"
# schema="farmlabv3_live"
# schema="historical"
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

comparison_df = pd.read_csv("inputFiles/client_approval_settings.csv")    
# comparison_df = comparison_df.loc[comparison_df['crop'] == 'Potatoes (Irish)']
reports_pivot_2 = pd.read_csv("inputFiles/reports_final.csv")

spectral_sample_df = pd.DataFrame()
conn = get_db_cursor()
cur = conn.cursor()
renaming = {}

renamed_opus_folder = "opus_renamed_2"
opus_folder = "opus_2"
spectral_sample_output_file = "Spectral Sample Output_2.csv"
for index, row in comparison_df.iterrows():
    os.makedirs(f"outputFiles/{renamed_opus_folder}",exist_ok=True)
    os.makedirs(f"outputFiles/{opus_folder}",exist_ok=True)
    barcode = row['barcode']
    print(barcode)
    if len([i for i in Path(f'./outputFiles/{renamed_opus_folder}').rglob(f"*{barcode}*")]) == 2:
        continue
    crop = row['crop']
    ph = row['ph']
    phosphorus = row['phosphorus']
    potassium = row['potassium']
    organic_matter = row['Organic Matter']
    calcium = row['calcium']
    magnesium = row['magnesium']
    
    try:
        aez = row['AEZ_name']
    except Exception as e:
        print(e)

    crop_id = pd.read_sql(f"SELECT id FROM crop c WHERE c.name = '{crop}'", con=conn).id.values[0]
    crop = crop.replace(" ","")
    print(crop)
    folder = f"./outputFiles/{crop}_ph-{ph}_phosphorus-{phosphorus}_potassium-{potassium}_organicmatter-{organic_matter}_calcium-{calcium}_magnesium-{magnesium}"
    
    os.makedirs(folder, exist_ok=True)
    print(f"==============================={crop_id}====================================")
    crop_reports = (reports_pivot_2.loc[reports_pivot_2['crop_id'] == str(crop_id)])
    print(f"==============================={len(crop_reports)}====================================")
    if(ph in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['ph'] == ph]
        print(f"===============================ph {len(crop_reports)}====================================")
    if(phosphorus in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['available_p'] == phosphorus]
        print(f"===============================p {len(crop_reports)}====================================")
    if(potassium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['exchangeable_k'] == potassium]
        print(f"===============================k {len(crop_reports)}====================================")
    if(calcium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['calcium'] == calcium]
        print(f"===============================ca {len(crop_reports)}====================================")
    if(magnesium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['magnesium'] == magnesium]
        print(f"===============================mg {len(crop_reports)}====================================")
    if(organic_matter in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['organic_matter'] == organic_matter]
        print(f"===============================om {len(crop_reports)}====================================")

    crop_reports.to_csv(f"{folder}/crop_reports.csv")
    if(len(crop_reports) == 0):
        print("No crop reports for {}".format(crop))
        continue
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
    TIMESTAMPDIFF(YEAR,'2024-17-05',date_of_planting) as 'Tree Age(Years)', 
    field_size AS 'Field Size (Acre)'
    FROM SpectralSample ss 
    WHERE barcode IN {','.join(str(crop_reports['barcode'].values[0:100]).replace('[','(').replace(']',')').split(' '))} AND ss.spectral_batch_id > 3355
    """, con=conn)
    
    
    print(f"======================{folder}======================")
    for index, row in spectral_df.iterrows():
        batch_no = row['spectral_batch_id']
        batch_no = str(batch_no).split('.')[0]
        print(f"Downloading batch no {batch_no}")
        if len([i for i in Path(f'./outputFiles/{renamed_opus_folder}').rglob(f"*{barcode}*")]) == 2:
            break
        try:
            subprocess.run(f"scp -r root@161.35.160.152:/mnt/volume_lon1_01/spc_backup/batch_{batch_no} {folder}")
            
            count = 0
            for file in [i for i in Path(folder).rglob("**/*.0")][::-1]:
                if(count==2):
                    break
                directory = file.parent
                name = file.name
                actual_barcode = name.split("_")[0]
                
                shutil.copyfile(file,f"outputFiles/{opus_folder}/{name}")
                os.rename(file,f"outputFiles/{renamed_opus_folder}/{barcode}_{count}.0")
                renaming[actual_barcode] = barcode
                row['Previous Crop'] = crop
                row['Next Crop'] = crop
                row['Other Crops'] = crop
                row['Barcode'] = barcode
                row['Report Language'] = 'en'
                _ = pd.DataFrame(row).T
                print(spectral_sample_df)
                spectral_sample_df = pd.concat([spectral_sample_df, _])
                print(spectral_sample_df)
                count+=1
            
        except Exception as e:
            print(e)
with open('renaming.json', 'w') as outfile:
    json.dump(renaming, outfile)
subprocess.run(f"powershell Compress-Archive {os.getcwd()}\\outputFiles\\{renamed_opus_folder} {os.getcwd()}\\outputFiles\\opus.zip")
spectral_sample_df = spectral_sample_df.drop_duplicates(subset="Barcode")
spectral_sample_df.to_csv(f"outputFiles/{spectral_sample_output_file}")   
    