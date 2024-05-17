
import pandas as pd
import numpy as np
import mariadb
import os
import shutil
import subprocess
from pathlib import Path
import mariadb
import sys


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

comparison_df = pd.read_csv("inputFiles/approval-check.csv")    
reports_pivot_2 = pd.read_csv("inputFiles/reports_final.csv")

conn = get_db_cursor()
cur = conn.cursor()
renaming = {}
for index, row in comparison_df.iterrows():
    os.makedirs("outputFiles/opus_renamed",exist_ok=True)
    os.makedirs("outputFiles/opus",exist_ok=True)
    barcode = row['barcode']
    if len([i for i in Path('./outputFiles/opus_renamed').rglob(f"*{barcode}*")]) == 2:
        continue
    print(barcode)
    crop = row['crop']
    ph = row['ph']
    phosphorus = row['phosphorus']
    potassium = row['potassium']
    organic_matter = row['Organic Matter']
    calcium = row['calcium']
    magnesium = row['magnesium']

    crop_id = pd.read_sql(f"SELECT id FROM crop c WHERE c.name = '{crop}'", con=conn).id.values[0]

    crop_reports = (reports_pivot_2.loc[reports_pivot_2['crop_id'] == str(crop_id)])
    if(ph in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['ph'] == ph]
    if(phosphorus in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['available_p'] == phosphorus]
    if(potassium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['exchangeable_k'] == potassium]
    if(calcium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['calcium'] == calcium]
    if(magnesium in ['low','very low','optimum','high','very high']):
        crop_reports = crop_reports.loc[crop_reports['magnesium'] == magnesium]
    if(len(crop_reports) == 0):
        continue
    spectral_df = pd.read_sql(f"SELECT spectral_sample_id, barcode, spectral_batch_id FROM SpectralSample WHERE barcode IN {tuple(crop_reports['barcode'].values)}", con=conn)
    batches = np.unique([i for i in spectral_df.spectral_batch_id])
    folder = f"./outputFiles/{crop}_ph-{ph}_phosphorus-{phosphorus}_potassium-{potassium}_organicmatter-{organic_matter}_calcium-{calcium}_magnesium-{magnesium}"
    os.makedirs(folder, exist_ok=True)
    print(f"======================{folder}======================")
    for batch_no in batches:
        print(f"Downloading batch no {batch_no}")
        if len([i for i in Path('./outputFiles/opus_renamed').rglob(f"*{barcode}*")]) == 2:
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
                
                shutil.copyfile(file,f"outputFiles/opus/{name}")
                os.rename(file,f"outputFiles/opus_renamed/{barcode}_{count}.0")
                renaming[actual_barcode] = barcode
                count+=1
            
        except Exception as e:
            print(e)

subprocess.run(f"powershell Compress-Archive {os.getcwd()}\\outputFiles\\opus_renamed {os.getcwd()}\\outputFiles\\opus.zip")
 

    