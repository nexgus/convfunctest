import cx_Oracle
import pandas as pd


class Default(object):
    host = "localhost"
    port = 49161  # 1521
    service = "xe"
    username = "system"
    password = "oracle"
    table = "conv_it_test"
    excel = "./data.xlsx"

    FIELDS = "SERIAL_NUMBER,MODEL_NAME,MO_NUMBER,STATION_NUMBER,IN_TIME,OUT_TIME,FORK_FLAG,LOG_TIME,LOG_SRC"


def main(args):
    host = f"{args.host}:{args.port}/{args.service}"
    conn = cx_Oracle.connect(args.username, args.password, host)
    cur = conn.cursor()

    cur.execute(f"TRUNCATE TABLE {args.table}")
    conn.commit()

    df = pd.read_excel(args.excel, sheet_name="Sheet1")
    idx = 0
    for _, row in df.iterrows():
        if pd.isnull(row.it_id):
            # ignore OT event
            continue

        try:
            station_number = int(row.it_id)
        except ValueError:
            # Skip non-DCT event
            continue

        idx += 1
        print(f"\r{idx}", end="", flush=True)

        # Save IT event to Oracle database
        value = (f"'{row.SERIAL_NUMBER}',"
                 f"'{row.MODEL_NAME}',"
                 f"'{row.MO_NUMBER}',"
                 f"{row.STATION_NUMBER},"
                 f"TO_DATE('{row.IN_TIME}','yyyy/mm/dd hh24:mi:ss'),"
                 f"TO_DATE('{row.OUT_TIME}','yyyy/mm/dd hh24:mi:ss'),"
                 f"{row.FORK_FLAG},"
                 f"TO_DATE('{row.LOG_TIME}','yyyy/mm/dd hh24:mi:ss'), "
                 f"'{row.LOG_SRC}'")
        sql = f"INSERT INTO {args.table} ({Default.FIELDS}) VALUES ({value})"
        try:
            cur.execute(sql)
        except Exception as e:
            print(sql)
            raise
        else:
            conn.commit()

    conn.close()
    print()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--host", "-H", type=str, default=Default.host,
                        help="Oracle database host.")
    parser.add_argument("--port", "-p", type=int, default=Default.port,
                        help="Oracle database port (usually 49161 or 1521)")
    parser.add_argument("--service", "-s", type=str, default=Default.service,
                        help="Oracle dtabase service name")
    parser.add_argument("--username", "-U", type=str, default=Default.username,
                        help="Oracle username")
    parser.add_argument("--password", "-P", type=str, default=Default.password,
                        help="Oracle password")
    parser.add_argument("--table", "-t", type=str, default=Default.table,
                        help="Oracle database table name for IT.")
    parser.add_argument("--excel", "-x", type=str, default=Default.excel,
                        help="Path to Excel data file.")
    args = parser.parse_args()

    main(args)
