import pandas as pd
import numpy as np
import os
import json
import mariadb
import sys

username = "scoring"
password = "idkltb93e0eomejp"
host = 'spectral-msql-jul-24-backup-do-user-2276924-0.b.db.ondigitalocean.com'
port = 25060
database="farmlabv3_live"

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

df = pd.read_csv("inputFiles/Apollo Agriculture Test Protocol.csv")

df.columns = [i.lower() for i in df.columns]
approval = pd.read_excel("inputFiles/batch-approval-batch_9366.xlsx",sheet_name="Recommendations")
approval.columns  = [i.lower() for i in approval.columns]
approval['crop_code'] = [str(i).split("-")[-1] for i in approval['barcode'] ]
approval['barcode'] = ["-".join(str(i).split("-")[:-1]) for i in approval['barcode'] ]

soil_correction_cols = [i for i in df.columns if "soil correction" in i]
soil_correction_cols.append("barcode")
soil_correction_requirements = df[soil_correction_cols]
soil_correction_requirements = soil_correction_requirements.set_index("barcode")

soil_correction_cols = [i for i in approval.columns if "soil correction" in i]
soil_correction_cols.append("barcode")
soil_correction_recommendations = approval[soil_correction_cols]
soil_correction_recommendations = soil_correction_recommendations.drop_duplicates(subset="barcode")
soil_correction_recommendations = soil_correction_recommendations.set_index("barcode")

soil_correction_recommendations = soil_correction_recommendations.reindex(soil_correction_requirements.index)

soil_correction_df = pd.DataFrame({"barcode":soil_correction_requirements.index})

#Handling soil correction
for col in soil_correction_requirements.columns:
    print(col)
    if("calcitic lime" in soil_correction_requirements[col].values):
        col_name = "soil correction:calcitic lime"
        a = soil_correction_requirements[[col]].isna()[col].values
        b = soil_correction_recommendations[[col_name]].isna()[col_name].values
        soil_correction_df[col_name] = np.logical_xor(a,b)
    if("dolomitic lime" in soil_correction_requirements[col].values):
        col_name = "soil correction:dolomitic lime"
        a = soil_correction_requirements[[col]].isna()[col].values
        b = soil_correction_recommendations[[col_name]].isna()[col_name].values
        soil_correction_df[col_name] = np.logical_xor(a,b)
    if("manure" in col):
        col_name = "soil correction:manure/compost **"
        try:
            a = (soil_correction_requirements[col])
            b = (soil_correction_recommendations[col_name])
        except:
            print(f"Ensure that the {col} is in both files")
        print(a)
        print(b)
        print(np.where(a == b, True, False))
        soil_correction_df[col_name] = np.where(a == b, True, False)       
soil_correction_df = soil_correction_df.replace(False,np.nan).replace(True,False)

planting_cols = [i for i in df.columns if "planting" in i]
planting_cols.append("barcode")
planting_requirements = df[planting_cols]
planting_requirements = planting_requirements.set_index("barcode")

planting_cols = [i for i in approval.columns if "planting" in i]
planting_cols.append("barcode")
planting_recommendations = approval[planting_cols]
planting_recommendations = planting_recommendations.drop_duplicates(subset="barcode")
planting_recommendations = planting_recommendations.set_index("barcode")
planting_recommendations = planting_recommendations.reindex(planting_requirements.index)

planting_fertilisers = []

planting_df = pd.DataFrame({"barcode":planting_requirements.index})
for col in planting_requirements.columns:
    print(col)
    reqs = np.unique(planting_requirements[col].dropna().values)
    if(len(reqs) > 1):
        raise Exception("Each column can only have one fertiliser type. Create a new column called PLANTING for extra fertiliser types.")
        break
    if(reqs in planting_fertilisers):
        raise Exception(f"{reqs} is already in another column. Ensure that each fertiliser type is only in one column.")
        break
    if(len(reqs) == 0):
        continue
    subtype = reqs[0]
    print(subtype)
    col_name = f"PLANTING:{subtype}".lower()
    print(col_name)
    planting_requirements = planting_requirements.rename(columns={col: col_name})
    
    a = planting_recommendations[[col_name]].isna()[col_name].values
    b = planting_requirements[[col_name]].isna()[col_name].values
    print(a)
    print(b)
    print(len(planting_requirements))
    print(len(planting_recommendations))
    planting_df[col_name] = np.logical_xor(a,b)
planting_df = planting_df.replace(False,np.nan).replace(True,False)
 

top_dress_cols = [i for i in df.columns if "top dress" in i]
top_dress_cols.append("barcode")
top_dress_requirements = df[top_dress_cols]
top_dress_requirements = top_dress_requirements.set_index("barcode")
top_dress_cols = [i for i in approval.columns if "top dress" in i]
top_dress_cols.append("barcode")
top_dress_recommendations = approval[top_dress_cols]
top_dress_recommendations = top_dress_recommendations.drop_duplicates(subset="barcode")
top_dress_recommendations = top_dress_recommendations.set_index("barcode")
top_dress_recommendations = top_dress_recommendations.reindex(top_dress_requirements.index)
top_dress_fertilizers = []

top_dress_df = pd.DataFrame({"barcode":top_dress_requirements.index})
for col in top_dress_requirements.columns:
    print(col)
    reqs = np.unique(top_dress_requirements[col].dropna().values)
    if(len(reqs) > 1):
        raise Exception("Each column can only have one fertiliser type. Create a new column called TOP DRESS for extra fertiliser types.")
        break
    if(reqs in top_dress_fertilizers):
        raise Exception(f"{reqs} is already in another column. Ensure that each fertiliser type is only in one column.")
        break
    if(len(reqs) == 0):
        continue
    subtype = reqs[0]
    print(subtype)
    col_name = f"TOP DRESS:{subtype}".lower()
    print(col_name)
    a = top_dress_recommendations[[col_name]].isna()[col_name].values
    b = top_dress_requirements[[col]].isna()[col].values
    print(a)
    print(b)
    print(len(top_dress_requirements))
    print(len(top_dress_recommendations))
    print(np.logical_xor(a,b))
    top_dress_df[col_name] = np.logical_xor(a,b)
top_dress_df = top_dress_df.replace(False,np.nan).replace(True,False)

final_output = pd.merge(soil_correction_df, planting_df, how="inner", on="barcode")
final_output = pd.merge(final_output, top_dress_df, how="inner", on="barcode")
final_output = final_output.set_index("barcode")

final_output['Approved'] = np.where(final_output.notnull().sum(axis=1) > 0, "fail","pass")


crops = pd.read_sql(f"SELECT id, LOWER(name) as name from crop c WHERE c.name IN {tuple(df['crop'])}",con=conn)
crops = crops.drop_duplicates(subset="name")
crops = crops.set_index("name")
crops_dict = crops.to_dict()

comparison_df = pd.read_csv("inputFiles/client_approval_settings.csv")    
comparison_df['crop'] = [ i.lower() for i in comparison_df['crop'].values]
comparison_df['id'] = [ crops_dict['id'][i] for i in comparison_df['crop'].values ]
comparison_df['barcode'] =  comparison_df['barcode'].astype("str")  +"-" + comparison_df['id'].astype("str")

spectral_sample_crop = pd.read_sql(f"SELECT barcode, report_data FROM SpectralSampleCrop WHERE barcode IN {tuple(comparison_df['barcode'].values)}",con=conn)
spectral_sample_crop  = spectral_sample_crop.set_index("barcode")

report_data = pd.DataFrame()
for index, row in spectral_sample_crop.iterrows():
    report = row['report_data']
    report = json.loads(report)
    results = report[index]['results']
    results = pd.DataFrame(results)
    results['barcode'] = index
    results = results.replace("Very Low",0).replace("Low",1).replace("Optimum",2).replace("High",3).replace("Very High",4)
    results = results.replace("Available P","phosphorus").replace("Exchangeable K","potassium")
    results['chemical_name'] = [ i.replace(" ","_").lower() for i in results['chemical_name'] ]
    results = results.dropna(subset="status")
    results.status = results.status.astype("int")
    report_data = pd.concat([report_data,results])

report_data = report_data.pivot_table(index="barcode", columns="chemical_name", values="status")
report_data = report_data.replace(0,"very low").replace(1,"low").replace(2,"optimum").replace(3,"high").replace(4,"very high")

report_data = report_data[['ph','phosphorus','potassium','organic_matter']]
comparison_df = comparison_df.loc[comparison_df['barcode'].isin(report_data.index)]
comparison_df = comparison_df.rename(columns={"Organic Matter": "organic_matter"})

reports = pd.DataFrame(index=report_data.index)

for col in report_data.columns:
    comparison = (np.array(comparison_df[col])  !=  np.array(report_data[col]))
    comparison_2 = [ not(i) for i in comparison ]
    reports[col] = comparison_2

reports.index = [ "-".join(i.split("-")[:-1]) for i in reports.index ]
reports = reports.replace(True, np.nan)

final_output = pd.merge(final_output, reports, left_index=True, right_index=True)

report_data.to_csv("outputFiles/reports.csv")
comparison_df.to_csv("outputFiles/comparison_df.csv")
final_output.to_csv("outputFiles/approval.csv")