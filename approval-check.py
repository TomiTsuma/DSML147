import pandas as pd
import numpy as np
import os

df = pd.read_csv("inputFiles/client_approval_settings.csv")
df.columns = [i.lower() for i in df.columns]
approval = pd.read_excel("inputFiles/batch_approval.xlsx",sheet_name="Recommendations")
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

final_output.to_csv("outputFiles/approval.csv")