import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
import time
import json


def latDD(x):
    if x == float('nan'):
        print("none")
        return float('nan')
    if type(x) == float:
        return x
    if ("W" in x) or ("S" in x) or ("w" in x) or ("s" in x):
        sign = -1
        print("Sign")
    else:
        sign = 1
    if '*' in x:
        x = re.sub(r'[^\d,]', '', x).replace(",", ".")
        return float(x)
    if '.' in x and "°" not in x and "˚" not in x and "/" not in x and "’" not in x:
        return float(x)
    if ',' in x and "°" not in x and " " not in x and "˚" not in x and "’" not in x:
        x = x.replace(",", ".")
        return float(x)
    if ',' in x and "°" not in x and " " in x and "˚" not in x and "/" not in x:
        x = x.replace(",", ".").replace(" ", '').replace("’", "")
        return float(x)
    if '/' in x:
        x = re.sub(r'[^\d,.]', '', x)
        D = int(x[0:2])
        M = int(x[3:5])
        S = float(x[5:].replace(",", "."))
        print(D)
        print(M)
        print(S)
        return D + float(M)/60 + float(S)/3600
    if ("W" in x) or ("S" in x) or ("w" in x) or ("s" in x):
        sign = -1
        print("Sign")
    else:
        sign = 1
    x = re.sub(r'[^\d,]', ' ', x).split()
    D = int(x[0])
    M = int(x[1])
    if len(x) > 2:
        S = float(x[2].replace(",", "."))
    else:
        S = 0
    DD = (D + float(M)/60 + float(S)/3600) * sign
    return DD


def date_formatter(row):
    try:
        return pd.to_datetime(row).timestamp()
    except:
        return None


def preprocess(reestr_valid):
    valid_data = []
    for i in tqdm(range(2, len(reestr_valid))):
        dolgota = reestr_valid.iloc[i]["Координаты загрязненного участка (в географической системе координат)"]
        shirota = reestr_valid.iloc[i]["Unnamed: 12"]
        registration_date = datetime.strptime(str(reestr_valid.iloc[i]["Дата регистрации в Реестре"]),
                                              '%Y-%m-%d %H:%M:%S').timetuple()
        square = float(reestr_valid.iloc[i]["Площадь загрязненного участка, га"])
        if type(shirota) != float and type(dolgota) != float:
            if ("W" in shirota or "E" in shirota or "w" in shirota or "e" in shirota) or \
                    ("S" in dolgota or "N" in dolgota or "s" in dolgota or "n" in dolgota):
                dolgota, shirota = shirota, dolgota
        dolgota = latDD(dolgota)
        shirota = latDD(shirota)
        pollutant = reestr_valid.iloc[i]["Вид приоритетного загрязняющего вещества"]
        district = reestr_valid.iloc[i]["Административный район"]
        valid_data.append((int(time.mktime(registration_date)), district, pollutant, dolgota, shirota, square))
    return valid_data


reestr = pd.read_excel("/Users/egor.smirnov/Desktop/РОСАТОМ/Reestr/Reestr-ZZ-na-18.05.2021.xlsx")
# смотрим только интересующие нас колонки
reestr_valid = reestr[reestr.columns[:20]]

reestr_valid = reestr_valid.dropna(subset=["Координаты загрязненного участка (в географической системе координат)",
                                           "Unnamed: 12"])

valid_data = preprocess(reestr_valid)

res_df = pd.DataFrame(valid_data, columns=["Registration_date", "District", "Pollutant", "Longitude", "Latitude",
                                           "Square"])

res_df['Rozliv'] = reestr_valid['Дата факта последнего разлива'].apply(date_formatter)

# второй лист
reestr_add = pd.read_excel("/Users/egor.smirnov/Desktop/РОСАТОМ/Reestr/Reestr-ZZ-na-18.05.2021.xlsx", sheet_name="Лист2")

valid_data_add = preprocess(reestr_add)
add_res = pd.DataFrame(valid_data_add, columns=["Registration_date", "District", "Pollutant", "Longitude", "Latitude",
                                                "Square"])

add_res['Rozliv'] = reestr_add['Дата факта последнего разлива'].apply(date_formatter)

res = pd.concat((res_df,add_res))
res = res.rename(columns={"Rozliv": "Rozliv_date"})
res = res.dropna(subset=["Longitude", "Latitude"])

res.to_csv("first_index.csv", index=False)

js = []
for i in range(len(res)):
    js.append({"Registration_date" : float(res.iloc[i]["Registration_date"]), "District" : res.iloc[i]["District"],
               "Pollutant" : res.iloc[i]["Pollutant"], "Longitude": res.iloc[i]["Longitude"],
               "Latitude": res.iloc[i]["Latitude"], "Square": res.iloc[i]["Square"],
               "Rozliv_date": res.iloc[i]["Rozliv_date"]})

with open("first_index.json", "wt") as f:
    json.dump(js, f)
