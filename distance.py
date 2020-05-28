# -*- coding: utf-8 -*-
"""Tutorial for using pandas and the InfluxDB client."""

import argparse
import pandas as pd
import json

from influxdb import InfluxDBClient
from datetime import datetime

from geopy.distance import geodesic, great_circle

def main(host='localhost', port=8089, drone_name=None):
    """Instantiate the connection to the InfluxDB client."""
    user = ''
    password = ''
    dbname = 'searchwing'

    client = InfluxDBClient(host, 8086, user, password, dbname, use_udp=True, udp_port=port)
    result = client.query(f'SELECT "Lat", "Lng" FROM "GPS" WHERE ("drone" =~ /^{drone_name}$/) ORDER BY time')

    coordinates = []
    last = None
    distance = None

    for point in result.get_points():
        time, lat, lng = point.values()
        current = lat, lng
        if not last:
            distance = great_circle(current, current)
        else:
            distance = distance + great_circle(last, current)
        last = current
    print(distance)

def parse_args():
    """Parse the args from main."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('drone', type=str,
                        help='name of the drone')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8089,
                        help='port of InfluxDB http API')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, drone_name=args.drone)
