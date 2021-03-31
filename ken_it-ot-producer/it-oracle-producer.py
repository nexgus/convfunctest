#!/usr/bin/env python3.9

import json
import sys
import os
from string import Template
from argparse import ArgumentParser
import random
from datetime import timedelta
import copy
import datetime
import string
import time
import textwrap

###########################################
#
# author: Kenduest Lee (kenduest@brobridge.com)
#
# Copyright by brobridge
#
###########################################

"""
Usage:

    $ ./it-oracle-producer.py \
            --server=localhost:1521 \
            --service-id=XE \
            --username=gravity \
            --password=gravity \ 
            --table=it_test \
            --truncate-table \
            --data-start-time="2021-03-20 12:33:44.12" \
            --data-amount=100000 \
            --show-every=1000 \
            --period-time=60 \
            --serial-prefix="TBSXN" \
            --serial-id-beginning=1



    $ ./it-oracle-producer.py  \
            --data-amount=100000 \
            --period-time=60 \
            --serial-id-beginning=1 \
            --dump-data-only


"""

"""
********** Oracle Enable logminer steps: **********

shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

CREATE TABLESPACE LOGMINER_TBS DATAFILE '/u01/app/oracle/oradata/XE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
CREATE USER logminer IDENTIFIED BY logminer DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs;
GRANT CREATE SESSION TO logminer;
GRANT SELECT ON V_$DATABASE TO logminer;
GRANT FLASHBACK ANY TABLE TO logminer;
GRANT SELECT ANY TABLE TO logminer;
GRANT SELECT_CATALOG_ROLE TO logminer;
GRANT EXECUTE_CATALOG_ROLE TO logminer;
GRANT SELECT ANY TRANSACTION TO logminer;
GRANT SELECT ANY DICTIONARY TO logminer;
GRANT CREATE TABLE TO logminer;
GRANT ALTER ANY TABLE TO logminer;
GRANT LOCK ANY TABLE TO logminer;

CREATE TABLESPACE gravity_tbs DATAFILE '/u01/app/oracle/oradata/XE/gravity_tbs.dbf' SIZE 100M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
CREATE USER gravity IDENTIFIED BY gravity DEFAULT TABLESPACE gravity_tbs QUOTA UNLIMITED ON gravity_tbs;
GRANT CONNECT, RESOURCE TO gravity;
GRANT UNLIMITED TABLESPACE TO gravity;
GRANT CREATE SEQUENCE to gravity;

connect gravity/gravity;

CREATE TABLE it_test (
   SERIAL_NUMBER	VARCHAR2(25) NOT NULL,
   MODEL_NAME	    VARCHAR2(25),
   MO_NUMBER	    VARCHAR2(25),
   STATION_NUMBER	NUMBER(10,0),
   IN_TIME	        DATE,
   OUT_TIME	        DATE,
   FORK_FLAG	    NUMBER(4,0),
   LOG_TIME	        DATE,
   LOG_SRC	        VARCHAR2(50)
);


ALTER TABLE gravity.it_test ADD ( CONSTRAINT atm_pk PRIMARY KEY (SERIAL_NUMBER) );
ALTER TABLE gravity.it_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;


"""

station_tables = [
    {"station_number": "600000001", "name": "HA_SMT_01",
        "model_name": "28Q000RED03X1", "mo_number": "H10-21010100"},
    {"station_number": "600000002", "name": "HA_SMT_03",
        "model_name": "28Q000S2201X1", "mo_number": "H20-21010200"},
    {"station_number": "600000003", "name": "HB_SMT_01",
        "model_name": "28Q000XXD03X1", "mo_number": "H30-21010300"},
    {"station_number": "600000004", "name": "HB_SMT_03",
        "model_name": "28Q000EDD03X1", "mo_number": "H40-21010400"},
    {"station_number": "600010310", "name": "HB_RAY_01",
        "model_name": "28Q000FGD03X1", "mo_number": "H50-21010500"},
]

table_field = "SERIAL_NUMBER,MODEL_NAME,MO_NUMBER,STATION_NUMBER,IN_TIME,OUT_TIME,FORK_FLAG,LOG_TIME,LOG_SRC"


class DefaultSettings:
    SERVER = "192.168.1.1"
    SERVICE_ID = "XE"
    USERNAME = "gravity"
    PASSWORD = "gravity"
    TABLE_NAME = "it_test"
    SERIAL_ID_BEGIN = 0

    DATA_AMOUNT = 100000  # default total progress amount
    PERIOD_TIME = 0
    PROGRESS_AMOUNT = 1000  #
    DATA_DELAY_INSERT_SECONDS = 0  # insert to oracle db delay time for each one
    COMMIT_DATA_EVERY = 10000  # oracle commit every amount


def main():
    parser = ArgumentParser()

    group1 = parser.add_argument_group('General server options')

    group1.add_argument(
        "--server",
        help="oracle server location (default: %s)" % DefaultSettings.SERVER,
        required=False,
        dest="oracle_server",
        default=DefaultSettings.SERVER,
        metavar="IP|Hostname[:port]"
    )

    group1.add_argument(
        "--service-id",
        help="oracle server service id or SSID (default: %s)" % DefaultSettings.SERVICE_ID,
        required=False,
        dest="oracle_service_id",
        default=DefaultSettings.SERVICE_ID,
        metavar="id",
    )

    group1.add_argument(
        "--table",
        help="oracle operation for table name (default: %s)" % DefaultSettings.TABLE_NAME,
        required=False,
        dest="table_name",
        default=DefaultSettings.TABLE_NAME,
        metavar="tablename",
    )

    group1.add_argument(
        "--username",
        help="oracle server login username (default: %s)" % DefaultSettings.USERNAME,
        required=False,
        dest="oracle_username",
        default=DefaultSettings.USERNAME,
        metavar="username",
    )

    group1.add_argument(
        "--password",
        help="oracle server login password (default: %s)" % DefaultSettings.PASSWORD,
        required=False,
        dest="oracle_password",
        default=DefaultSettings.PASSWORD,
        metavar="password",
    )

    group1.add_argument(
        "--truncate-table",
        help="truncate db table first",
        required=False,
        dest="truncate_table",
        default=False,
        action="store_true",
    )

    group2 = parser.add_argument_group('Data handle options')

    group2.add_argument(
        "--data-amount",
        help="total data amount (0: for unlimited, default: %s)" % DefaultSettings.DATA_AMOUNT,
        type=int,
        required=False,
        dest="data_amount",
        default=DefaultSettings.DATA_AMOUNT,
        metavar="value"
    )

    group2.add_argument(
        "--serial-prefix",
        help="data serial prefix string (default is auto-generated unique one for runtime)",
        type=str,
        required=False,
        dest="serial_prefix",
        default=''.join(random.choice(string.ascii_uppercase)
                        for i in range(8)),
        metavar="prefix"
    )

    group2.add_argument(
        "--serial-id-beginning",
        help="data serial prefix string (default: %d)" % DefaultSettings.SERIAL_ID_BEGIN,
        type=int,
        required=False,
        dest="serial_id_beginning",
        default=DefaultSettings.SERIAL_ID_BEGIN,
        metavar="value"
    )

    group2.add_argument(
        "--data-start-time",
        help="data in/out time for baseline for timestamp (ex: %s, default is now)" % datetime.datetime.now(
        ),
        required=False,
        type=str,
        dest="data_start_time",
        default=str(datetime.datetime.now()),
        metavar="YYYY-mm-dd H:M:S[.ss]",
    )

    group2.add_argument(
        "--data-insert-delay",
        help="data delay time for each insert (default: %s)" % DefaultSettings.DATA_DELAY_INSERT_SECONDS,
        required=False,
        type=int,
        dest="data_insert_delay",
        default=DefaultSettings.DATA_DELAY_INSERT_SECONDS,
        metavar="value",
    )

    group2.add_argument(
        "--commit-data-every",
        help="commit oracle data every amount (default: %s)" % DefaultSettings.COMMIT_DATA_EVERY,
        required=False,
        type=int,
        dest="commit_data_every",
        default=DefaultSettings.COMMIT_DATA_EVERY,
        metavar="value",
    )

    group3 = parser.add_argument_group('Data extra time options')

    group3.add_argument(
        "--period-time",
        help="run program within seconds (0: unlimited, default: %s)" % DefaultSettings.PERIOD_TIME,
        required=False,
        type=float,
        dest="period_time",
        default=DefaultSettings.PERIOD_TIME,
        metavar="value",
    )

    group3.add_argument(
        "--show-every",
        help="show processing amount per amount (0: disabled, default: %s)" % DefaultSettings.PROGRESS_AMOUNT,
        required=False,
        type=int,
        dest="show_every",
        default=DefaultSettings.PROGRESS_AMOUNT,
        metavar="value",
    )

    group4 = parser.add_argument_group('Other options')

    group4.add_argument(
        "--dump-data-only",
        help="dump data only, no oracle operation and ignore period time",
        action="store_true",
        dest="dump_data_only",
    )

    args = parser.parse_args()

    try:
        handle_oracle(args)
    except Exception as e:
        sys.stderr.write("\nSystem fail: %s\n\n" % str(e))
        exit(1)


def action_truncate_table(cur, table_name, verbose=True):
    if verbose:
        sys.stdout.write("Truncate table..")
    delete_sql = "truncate table %s" % table_name
    cur.execute(delete_sql)

    if verbose:
        sys.stdout.write("done\n\n")


def compose_random_data(serial_prefix, serial_id_start, timestamp):
    number = serial_id_start
    datetime_result = datetime.datetime.fromtimestamp(timestamp)
    in_time_result = out_time_result = log_time_result = datetime_result

    random_time_table1 = [random.uniform(10, 30) for i in range(30)]
    random_time_table2 = [random.uniform(10, 10) for i in range(30)]

    while True:
        station_result = random.choice(station_tables)

        serial_result = "%s%08d" % (serial_prefix, number)
        number += 1

        inc1 = random.choice(random_time_table1)
        inc2 = random.choice(random_time_table2)

        in_time_result += timedelta(seconds=inc1)
        out_time_result += timedelta(seconds=inc1 + inc2)
        log_time_result += timedelta(seconds=inc1 + inc2)

        yield ",".join([
            "'%s'" % serial_result,
            "'%s'" % station_result["model_name"],
            "'%s'" % station_result["mo_number"],
            "'%s'" % station_result["station_number"],
            "TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')" % in_time_result.strftime("%Y-%m-%d %H:%M:%S"),
            "TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')" % out_time_result.strftime("%Y-%m-%d %H:%M:%S"),
            "0",
            "TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')" % log_time_result.strftime("%Y-%m-%d %H:%M:%S"),
            "'%s'" % station_result["name"],
        ]
        )


def handle_oracle(args):
    data_start_time = args.data_start_time
    serial_prefix = args.serial_prefix
    show_every = args.show_every
    commit_data_every = args.commit_data_every
    truncate_table = args.truncate_table
    table_name = args.table_name
    data_amount = args.data_amount
    period_time = args.period_time
    oracle_username = args.oracle_username
    oracle_password = args.oracle_password
    oracle_conn_name = "%s/%s" % (args.oracle_server, args.oracle_service_id)
    serial_id_beginning = args.serial_id_beginning
    dump_data_only = args.dump_data_only
    data_insert_delay = args.data_insert_delay

    if len(serial_prefix) < 0:
        sys.stderr.write("\nError: Prefix length must > 0\n")
        sys.exit(1)

    if data_amount < 0:
        sys.stderr.write(
            "\nError: incorrect data amount: %s\n\n" % data_amount)
        sys.exit(1)

    if show_every < 0:
        sys.stderr.write(
            "\nError: incorrect show progress second: %s\n\n" % show_every)
        sys.exit(1)

    if commit_data_every < 0:
        sys.stderr.write(
            "\nError: incorrect commit value: %s\n\n" % commit_data_every)
        sys.exit(1)

    if serial_id_beginning < 0:
        sys.stderr.write(
            "\nError: incorrect beginning value: %s\n\n" % serial_id_beginning)
        sys.exit(1)

    if data_insert_delay < 0:
        sys.stderr.write(
            "\nError: incorrect insert delay time: %s\n\n" % data_insert_delay)
        sys.exit(1)

    try:
        if data_start_time == "":
            data_start_time = datetime.datetime.now().timestamp()
        else:
            try:
                data_start_time = datetime.datetime.strptime(
                    data_start_time, "%Y-%m-%d %H:%M:%S.%f").timestamp()
            except:
                data_start_time = datetime.datetime.strptime(
                    data_start_time, "%Y-%m-%d %H:%M:%S").timestamp()
    except:
        sys.stderr.write("Error: Invalid data start time: %s\n\n" %
                         args.data_start_time)
        sys.exit(1)

    sys.stdout.write("\n{x} Envionment {x}\n\n".format(x="*" * 10))
    sys.stdout.write("1. Using oracle server: %s (%s)\n" %
                     (oracle_conn_name, table_name))
    sys.stdout.write("2. Data Serial id prefix: %s (id from: %d)\n" %
                     (serial_prefix, serial_id_beginning))
    sys.stdout.write("2. Data timestamp start: %s\n" %
                     datetime.datetime.fromtimestamp(data_start_time))
    sys.stdout.write("3. Data amount: %s \n" %
                     (data_amount if data_amount > 0 else "N/A"))
    sys.stdout.write("4. Commit data every: %d \n" %
                     (commit_data_every if commit_data_every > 0 else "N/A"))
    sys.stdout.write("4. Insert data delay every: %s\n" %
                     (data_insert_delay if data_insert_delay > 0 else "N/A"))
    sys.stdout.write("5. Program run within Time: %s (seconds)\n" %
                     (period_time if period_time > 0 else "N/A"))

    sys.stdout.flush()

    if dump_data_only:
        current_data_amount = 0
        for item in compose_random_data(serial_prefix=serial_prefix,
                                        serial_id_start=serial_id_beginning,
                                        timestamp=data_start_time):

            sql = "INSERT INTO %s (%s) VALUES (%s)" % (
                table_name, table_field, str(item))
            sys.stdout.write("\n%s\n" % sql)

            current_data_amount += 1
            if data_amount > 0 and current_data_amount >= data_amount:
                break
        return

    oracle_connectin = None
    oracle_cursor = None

    import cx_Oracle

    try:
        sys.stdout.write("\nConnect to oracle....")
        oracle_connectin = cx_Oracle.connect(
            oracle_username, oracle_password, oracle_conn_name)
        oracle_cursor = oracle_connectin.cursor()
        sys.stdout.write("OK !\n\n")

    except Exception as e:
        sys.stderr.write("\nError: Oracle Error: %s\n" % str(e))
        sys.exit(1)

    if truncate_table:
        action_truncate_table(oracle_cursor, table_name)
        oracle_connectin.commit()

    start_timestamp = datetime.datetime.now().timestamp()
    current_data_amount = 0
    used_time = 0

    sys.stdout.write("Oracle Operation in progress now...\n\n")

    for item in compose_random_data(serial_prefix=serial_prefix, serial_id_start=serial_id_beginning,
                                    timestamp=data_start_time):

        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            table_name, table_field, str(item))
        oracle_cursor.execute(sql)

        current_data_amount += 1

        if data_insert_delay > 0:
            time.sleep(data_insert_delay)

        if commit_data_every > 0 and current_data_amount % commit_data_every == 0:
            oracle_connectin.commit()

        used_time = datetime.datetime.now().timestamp() - start_timestamp

        if period_time > 0 and used_time >= period_time:
            break

        if show_every > 0 and current_data_amount % show_every == 0:
            if data_amount == 0:
                sys.stdout.write("Current Progress amount: %d\r" %
                                 (current_data_amount,))
                sys.stdout.flush()
            else:
                sys.stdout.write("Current Progress amount: %d/%d\r" %
                                 (current_data_amount, data_amount))
                sys.stdout.flush()
        if data_amount > 0 and current_data_amount >= data_amount:
            break

    oracle_connectin.commit()

    if show_every:
        if data_amount == 0:
            sys.stdout.write("\rCurrent Progress amount: %d/%s\r\n\n" %
                             (current_data_amount, data_amount))
        else:
            sys.stdout.write("\rCurrent Progress amount: %d\r\n\n" %
                             current_data_amount)
            sys.stdout.write("Total required total amount: %s\n" % data_amount)
    sys.stdout.write("Total completed batch amount: %d\n" %
                     current_data_amount)
    sys.stdout.write("Total progress used seconds: %.6f\n\n" % used_time)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
