import os
import pandas as pd
from extracts_titanic import extract_data

def transform_data(raw_path):
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir=os.path.join(base_dir,"data","staged")
    os.makedirs(staged_dir,exist_ok=True)
    df=pd.read_csv(raw_path)
    # 1.Handle Missing Values
    numeric_cols = ["survived", "pclass", "age", "sibsp", "parch", "fare"]
    categorical_cols = ["sex", "embarked", "class", "who", "deck", 
                        "embark_town", "alive", "alone"]
    #filling the missing values with median
    for col in numeric_cols:
        df[col]=df[col].fillna(df[col].median())

    for col in categorical_cols:
        df[col] = df[col].fillna(df[col].mode()[0])
    #2.Feature Engineering 

    df["family_size"] = df["sibsp"] + df["parch"] + 1
    df["is_child"] = (df["age"] < 18).astype(int)
    df["fare_per_person"] = df["fare"] / df["family_size"]
    df["sex_num"] = df["sex"].map({"male": 1, "female": 0})
    #3.Drop unnecessary columns
    colss=[c for c in ["PassengerId","Name","Ticket","Cabin","adult_male"] if c in df.columns]
    df.drop(columns=colss, inplace=True, errors="ignore")
    #4.Saved data
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"Data transformed and saved at :{staged_path}")
    return staged_path
    
if __name__=="__main__":
    from extracts_titanic import extract_data
    raw_path=extract_data()
    transform_data(raw_path)