import argparse
import json
import numpy as np
import requests
import pandas as pd

from datetime import datetime

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--convdbman", type=str, default="api.conv-db-man.svc.cluster.local:80",
                    help="ConvDBManAPI IP:PORT")
parser.add_argument("--convsenormap", type=str, default="api.conv-sensormap.svc.cluster.local:80",
                    help="ConvSensorMapAPI IP:PORT")
parser.add_argument("--input", "-i", type=str, default="./it_data.xlsx",
                    help="Path to IT example data (Excel) file.")
parser.add_argument("--output", "-o", type=str, default="./data.xlsx",
                    help="Path to output dataset (Excel) file.")
parser.add_argument("--minimum", "-M", type=float, default=60.0,
                    help="Minimum interval between two IT event with the same DCT ID.")
args = parser.parse_args()


# Get all valid DCT station IDs
resp = requests.get(f"http://{args.convdbman}/api/v1/dct")
valid_dct = list(json.loads(resp.content.decode("utf-8")).keys())

# Read IT example data
df = pd.read_excel(args.input, sheet_name="Sheet1")
df["STATION_NUMBER"] = df["STATION_NUMBER"].astype(str)

sn_list = []
duplicated = []
for index, row in df.iterrows():
    in_time = datetime.strptime(row.IN_TIME, "%Y-%m-%d %H:%M:%S")
    out_time = datetime.strptime(row.OUT_TIME, "%Y-%m-%d %H:%M:%S")
    log_time = datetime.strptime(row.LOG_TIME, "%Y-%m-%d %H:%M:%S")
    df.at[index, "IN_TIME"] = in_time
    df.at[index, "OUT_TIME"] = out_time
    df.at[index, "LOG_TIME"] = log_time

    df.at[index, "duplicated"] = False
    df.at[index, "ignore"] = False

    sn = row.SERIAL_NUMBER
    if row.STATION_NUMBER in valid_dct:
        if sn not in sn_list:
            sn_list.append(sn)
            continue
        if sn not in duplicated:
            duplicated.append(sn)

df.loc[df.SERIAL_NUMBER.isin(duplicated) & df.STATION_NUMBER.isin(
    valid_dct), "duplicated"] = True

for sn in duplicated:
    indices = df.index[(df["duplicated"] == True) & (
        df["SERIAL_NUMBER"] == sn)].tolist()

    for i in range(len(indices)-1):
        cur_index = indices[i]
        nxt_index = indices[i+1]
        t_cur = df.iloc[cur_index].IN_TIME
        t_nxt = df.iloc[nxt_index].IN_TIME
        t_diff = (t_nxt - t_cur).total_seconds()
        if t_diff < args.minimum:
            df.at[cur_index, "ignore"] = True

df = df.drop(["duplicated"], axis=1)

columns = ["ts", "it_id", "ot_idx", "ot_val", "history"] + list(df.columns)
df_new = pd.DataFrame(columns=columns)


def dfappend(**kwargs):
    global df_new, columns
    values = []
    for key in columns:
        if key in kwargs:
            values.append(kwargs[key])
        else:
            values.append(None)

    idx = df_new.shape[0] + 1
    df_new.loc[idx] = values


# Generate IT Events
history = dict()
total = df.shape[0]
it_idx = 0
for _, row in df.iterrows():
    it_idx += 1
    print(f"\rIT: {it_idx}/{total}", end="", flush=True)
    dct_id = row.STATION_NUMBER
    sn = row.SERIAL_NUMBER
    ts0 = datetime.timestamp(row.IN_TIME)

    if not row.ignore and dct_id in valid_dct:
        if sn in history:
            history[sn]["hidx"] = history[sn]["hidx"] + 1
            history[sn]["dct_id"] = dct_id
        else:
            history[sn] = {
                "hidx": 0,
                "dct_id": dct_id,
            }

        dfappend(ts=ts0, it_id=dct_id,
                 history=history[sn]["hidx"],
                 SERIAL_NUMBER=row.SERIAL_NUMBER,
                 MODEL_NAME=row.MODEL_NAME,
                 MO_NUMBER=row.MO_NUMBER,
                 STATION_NUMBER=int(dct_id),
                 IN_TIME=row.IN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 OUT_TIME=row.OUT_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 FORK_FLAG=row.FORK_FLAG,
                 LOG_TIME=row.LOG_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 LOG_SRC=row.LOG_SRC,
                 ignore=row.ignore)

        resp = requests.get(f"http://{args.convdbman}/dct/{dct_id}")
        dct = json.loads(resp.content.decode("utf-8"))
        workstations = dct["workstations"]

        resp = requests.get(f"http://{args.convsensormap}/record/{dct_id}")
        sensormap = json.loads(resp.content.decode("utf-8"))

        for ws_id in workstations:
            offset = sensormap["work_station"][ws_id]["offset"]
            ts = ts0 + offset
            dfappend(ts=ts, it_id=ws_id,
                     history=history[sn]["hidx"], SERIAL_NUMBER=sn)

    else:
        dfappend(ts=ts0, it_id=dct_id,
                 SERIAL_NUMBER=row.SERIAL_NUMBER,
                 MODEL_NAME=row.MODEL_NAME,
                 MO_NUMBER=row.MO_NUMBER,
                 STATION_NUMBER=int(dct_id),
                 IN_TIME=row.IN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 OUT_TIME=row.OUT_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 FORK_FLAG=row.FORK_FLAG,
                 LOG_TIME=row.LOG_TIME.strftime("%Y-%m-%d %H:%M:%S"),
                 LOG_SRC=row.LOG_SRC,
                 ignore=row.ignore)

df_new = df_new.sort_values(by=["ts"], ascending=True)
print()

# Generate OT timestamps
total = df_new.shape[0]
ot_ts_max = df_new.iloc[total-1].ts + 30.5
ot_ts = df_new.iloc[0].ts - 30.5
ot_val = 0
while ot_ts <= ot_ts_max:
    ts = ot_ts
    ot_val += 1
    for ot_idx in range(13):
        dfappend(ts=ts, ot_idx=ot_idx, ot_val=ot_val)
        print(f"\rOT: {ot_val}", end="", flush=True)
        ts += 0.01
    ot_ts += 10
df_new = df_new.sort_values(by=["ts"], ascending=True)
print()

# Add OT values for IT event
total = df_new.shape[0]
prv_ot_val = None
for idx, row in df_new.iterrows():
    print(f"\rIT/OT: {idx}/{total}", end="")
    if pd.isnull(row.it_id):
        # this is an OT event
        prv_ot_val = row.ot_val
    else:
        # this is an IT event
        if prv_ot_val is None:
            continue

        try:
            int(row.it_id)
        except ValueError:
            # This is a workstation
            df_new.at[idx, "ot_val"] = prv_ot_val
        else:
            # This is a DCT station
            if row.it_id in valid_dct:
                df_new.at[idx, "ot_val"] = prv_ot_val
print()

df_new.to_excel(args.output, index=False)
