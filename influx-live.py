# -*- coding: utf-8 -*-
"""Tutorial for using pandas and the InfluxDB client."""

import argparse
import json

from influxdb import InfluxDBClient
from pymavlink import mavutil
from datetime import datetime, tzinfo, timezone

def generate_json(drone_name, message, fields):
    time = datetime.fromtimestamp(message._timestamp, timezone.utc)
    json_body = {
            "tags": {
                "drone": drone_name
                },
            "points":[
                {
                    "measurement": message.get_type(),
                    "time": time.isoformat(),
                    "fields": {}
                    }
                ]
            }

    for field in fields:
        json_body['points'][0]['fields'][field] = getattr(message, field)

    return json_body

def main(host='localhost', port=8089, drone_name=None, input_file=None):
    """Instantiate the connection to the InfluxDB client."""
    user = ''
    password = ''
    dbname = 'searchwing'

    print(f'Starting import for drone {drone_name}.')

    client = InfluxDBClient(host, 8086, user, password, dbname, use_udp=True, udp_port=port)

    connection = mavutil.mavlink_connection(input_file)

    while True:
        message = connection.recv_msg()
        if not message:
            continue
        if message.get_type() == "BATTERY_STATUS":
            time = datetime.fromtimestamp(message._timestamp, timezone.utc)
            json_body = {
                    "tags": {
                        "drone": drone_name
                        },
                    "points":[
                        {
                            "measurement": "BATTERY_STATUS",
                            "time": time.isoformat(),
                            "fields": {

                                "voltage": message.voltages[0],
                                "current_battery": message.current_battery
                                }
                            }
                        ]
                    }
            print(json_body)
            client.send_packet(json_body)
        elif message.get_type() == "GLOBAL_POSITION_INT":
            json_body = generate_json(
                    drone_name,
                    message,
                    ['lat', 'lon', 'alt', 'relative_alt', 'vx', 'vy', 'vz', 'hdg']
                    )
            print(json_body)
            client.send_packet(json_body)



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
