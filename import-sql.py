# -*- coding: utf-8 -*-

import argparse

from influxdb import InfluxDBClient
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


def generate_json(drone_name, message):
    time = datetime.fromtimestamp(message._timestamp, timezone.utc)
    data = message.to_dict()
    if data['mavpackettype'] == 'FMT':
        #print(data)
        table_name = data['Name']
        column_types = [FORMAT_TO_TYPE[c] for c in data['Format']]
        columns = data['Columns'].split(",")
        sql_columns = [f'"{c}" {t}' if t != "VARCHAR(255)"  else f'"{c}" {t}' for c, t in zip(columns, column_types)]
        print(f'CREATE TABLE IF NOT EXISTS {table_name} ( {", ".join(sql_columns)} );')
        #print(data)
    #for field in fields:
    #    json_body['points'][0]['fields'][field] = getattr(message, field)
    else:
        table_name = data['mavpackettype']
        del data['mavpackettype']
        columns = [f'"{k}"' for k in data.keys()]
        values = [str(message._timestamp)] + [f'\'{v}\'' if isinstance(v, str) else str(v) for v in data.values()][1:]
        #values = map(str, data.values())
        print(f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({",".join(values)});')
    #return json_body


def main(host='localhost', port=8089, drone_name=None, input_file=None):
    """Instantiate the connection to the InfluxDB client."""
    user = ''
    password = ''
    dbname = 'searchwing'

    #print(f'Starting import for drone {drone_name}.')

    #client = InfluxDBClient(host, 8086, user, password, dbname, use_udp=True, udp_port=port)

    connection = mavutil.mavlink_connection(input_file)

    print("BEGIN TRANSACTION;")
    while message := connection.recv_msg():
        json_body = generate_json(drone_name, message)
        #print(json_body)
        #client.send_packet(json_body)

    print("END TRANSACTION;")
    #print(f'Import for drone {drone_name} done.')


def parse_args():
    """Parse the args from main."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('drone', type=str,
                        help='name of the drone')
    parser.add_argument('input', type=str,
                        help='path to the input file')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8089,
                        help='port of InfluxDB http API')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, drone_name=args.drone, input_file=args.input)
