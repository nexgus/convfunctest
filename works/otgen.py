import json
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError


class Default(object):
    broker = "10.1.5.45:9094"
    topic = "conv_function_test"
    excel = "./data.xlsx"
    group = "./sensor_grouping.json"


def main(args):
    with open(args.group) as fp:
        sensor_group = json.load(fp)

    #producer = KafkaProducer(bootstrap_servers=[args.broker])
    producer = KafkaProducer(
        bootstrap_servers=[args.broker],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"))

    df = pd.read_excel(args.excel, sheet_name="Sheet1")
    total = df['it_id'].isnull().sum()
    idx = 0
    for _, row in df.iterrows():
        if not pd.isnull(row.it_id):
            # ignore IT event
            continue

        idx += 1
        print(f"\r{idx}/{total}", end="", flush=True)

        msg = {"Time": row.ts}
        ot_idx = int(row.ot_idx)
        sensors = sensor_group[ot_idx]
        for sensor in sensors:
            msg[sensor] = row.ot_val
        if ot_idx == 9:
            # This is legacy data format
            msg = {"info": msg}

        #msg = json.dumps(msg).encode("utf-8")
        future = producer.send(args.topic, msg)

    # block until all async messages are sent
    print()
    print("Flushing async messages...", end="", flush=True)
    producer.flush()

    producer.close()
    print(" done")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--broker", "-b", type=str, default=Default.broker,
                        help="Kafka broker IP:PORT.")
    parser.add_argument("--topic", "-t", type=str, default=Default.topic,
                        help="OT topic.")
    parser.add_argument("--excel", "-x", type=str, default=Default.excel,
                        help="Path to Excel data file.")
    parser.add_argument("--group", "-G", type=str, default=Default.group,
                        help="Path to OT sensor group file.")
    args = parser.parse_args()

    main(args)
