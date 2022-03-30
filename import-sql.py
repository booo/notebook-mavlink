# -*- coding: utf-8 -*-

import argparse

from pymavlink import mavutil
from datetime import datetime, timezone

FORMAT_TO_TYPE = {
    "a": "VARCHAR(255)",
    "b": "INTEGER",
    "B": "INTEGER",
    "h": "INTEGER",
    "H": "INTEGER",
    "i": "INTEGER",
    "I": "INTEGER",
    "f": "DOUBLE PRECISION",
    "n": "VARCHAR(255)",
    "N": "VARCHAR(255)",
    "Z": "VARCHAR(255)",
    "c": "DOUBLE PRECISION",
    "C": "DOUBLE PRECISION",
    "e": "DOUBLE PRECISION",
    "E": "DOUBLE PRECISION",
    "L": "DOUBLE PRECISION",
    "d": "DOUBLE PRECISION",
    "M": "INTEGER",
    "q": "INTEGER",
    "Q": "INTEGER"
    }


def generate_sql(message):
    data = message.to_dict()
    if data['mavpackettype'] == 'FMT':
        table_name = data['Name']
        column_types = [FORMAT_TO_TYPE[c] for c in data['Format']]
        columns = data['Columns'].split(",")
        sql_columns = [f'"{c}" {t}' if t != "VARCHAR(255)"  else f'"{c}" {t}' for c, t in zip(columns, column_types)]
        print(f'CREATE TABLE IF NOT EXISTS {table_name} ( {", ".join(sql_columns)} );')
    else:
        table_name = data['mavpackettype']
        del data['mavpackettype']
        columns = [f'"{k}"' for k in data.keys()]
        values = [str(message._timestamp)] + [f'\'{v}\'' if isinstance(v, str) else str(v) for v in data.values()][1:]
        print(f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({",".join(values)});')


def main(input_file=None):

    connection = mavutil.mavlink_connection(input_file)

    print("BEGIN TRANSACTION;")
    while message := connection.recv_msg():
        json_body = generate_sql(message)
    print("END TRANSACTION;")


def parse_args():
    """Parse the args from main."""
    parser = argparse.ArgumentParser(
        description='example code to generate sql tables from dataflash log')
    parser.add_argument('input', type=str,
                        help='path to the input file')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(input_file=args.input)
