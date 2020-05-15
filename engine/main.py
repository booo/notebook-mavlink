import numpy as np

from bokeh.driving import count
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, Select, Slider
from bokeh.plotting import curdoc, figure

import datetime

np.random.seed(1)



source = ColumnDataSource(dict(
    time=[], volt=[], curr=[]
))

tools="xpan,xwheel_zoom,xbox_zoom"

p = figure(plot_height=500, tools="xpan,xwheel_zoom,xbox_zoom,reset", x_axis_type=None, y_axis_location="right")
p.x_range.follow = "end"
p.x_range.follow_interval = 100
p.x_range.range_padding = 0

p2 = figure(plot_height=250, x_range=p.x_range, tools=tools, y_axis_location="right")
p2.line(x='time', y='volt', color='red', source=source)
p2.line(x='time', y='curr', color='blue', source=source)


from pymavlink import mavutil
#connection = mavutil.mavlink_connection("./data.bin")
connection = mavutil.mavlink_connection("tcp:192.168.8.156:23")

@count()
def update(t):

    #BAT {TimeUS : 240194792, Volt : 29.808740615844727, VoltR : 30.286956787109375, Curr : 1.311611533164978, CurrTot : 90.03011322021484, EnrgTot : 2.6887550354003906, Temp : 0.0, Res : 0.364601731300354}
    message = connection.recv_msg()

    if message:
        if message.get_type() == 'BAT':
            new_data = dict(
                time=[message.TimeUS],
                volt=[message.Volt],
                curr=[message.Curr]
            )
            source.stream(new_data, 10**5)

        if message.get_type() == 'BATTERY_STATUS':
            new_data = dict(
                time=[datetime.datetime.now()],
                volt=[message.voltages[0] / 1000],
                curr=[message.current_battery / 1000]
            )
            source.stream(new_data, 10**5)

curdoc().add_root(column(p2))
curdoc().add_periodic_callback(update, 1)
curdoc().title = "Battery"
