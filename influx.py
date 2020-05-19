# -*- coding: utf-8 -*-
"""Tutorial for using pandas and the InfluxDB client."""

import argparse
import pandas as pd
import json

from influxdb import InfluxDBClient
from pymavlink import mavutil
from datetime import datetime

def main(host='localhost', port=8089, drone_name=None, input_file=None):
    """Instantiate the connection to the InfluxDB client."""
    user = ''
    password = ''
    dbname = 'searchwing'

    print(f'Starting import for drone {drone_name}.')

    client = InfluxDBClient(host, 8086, user, password, dbname, use_udp=True, udp_port=port)

    connection = mavutil.mavlink_connection(input_file)

    while message := connection.recv_msg():
        time = datetime.fromtimestamp(message._timestamp)
        if message.get_type() == "BAT":
            json_body = {
                    "tags": {
                        "drone": drone_name
                        },
                    "points":[
                        {
                            "measurement": "BAT",
                            "time": time.isoformat(),
                            "fields": {
                                "Volt": message.Volt,
                                "Curr": message.Curr
                                }
                            }
                        ]
                    }
            #print(client.write_points(json_body))
            client.send_packet(json_body)
        elif message.get_type() == "GPS":
            print(message)
            json_body = {
                    "tags": {
                        "drone": drone_name
                        },
                    "points":[
                        {
                            "measurement": "GPS",
                            "time": time.isoformat(),
                            "fields": {
                                "Spd": message.Spd,
                                "Lat": message.Lat,
                                "Lng": message.Lng,
                                "Alt": message.Alt,
                                }
                            }
                        ]
                    }
            client.send_packet(json_body)
        elif message.get_type() == "ATT":
            json_body = {
                    "tags": {
                        "drone": drone_name,
                        },
                    "points":[
                        {
                            "measurement": "ATT",
                            "time": time.isoformat(),
                            "fields": {
                                "Roll": message.Roll,
                                "Pitch": message.Pitch,
                                "Yaw": message.Yaw,
                                "DesRoll": message.DesRoll,
                                "DesPitch": message.DesPitch,
                                "DesYaw": message.DesYaw,
                                "ErrRP": message.ErrRP,
                                "ErrYaw": message.ErrYaw
                                }
                            }
                        ]
                    }
            client.send_packet(json_body)

    print(f'Import for drone {drone_name} done.')


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
