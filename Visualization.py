import pandas
import matplotlib.pyplot as plt

import numpy as np

# Read output csv files and put them into dataframe
df = pandas.read_csv(r'C:\Users\liqih\Desktop\after.csv', delimiter = ',' , names = ['center','datapoint_x', 'datapoint_y'])

# A fuction used to define colors from different center
def pltcolor(list):
    cols=[]
    for record in list:
        if record == 'A':
            cols.append('red')
        elif record == 'B':
            cols.append('blue')
        elif record == 'C':
            cols.append('yellow')
        elif record == 'D':
            cols.append('black')
        elif record == 'E':
            cols.append('navy')
        elif record == 'F':
            cols.append('plum')
        elif record == 'G':
            cols.append('pink')
        else:
            cols.append('green')
    return cols

# Drop outliers
df = df.drop(df[df.datapoint_x < -74.10].index)
df = df.drop(df[df.datapoint_x > -73.80].index)
df = df.drop(df[df.datapoint_y < 40.50].index)
df = df.drop(df[df.datapoint_y > 41].index)

# Call color function
cols=pltcolor(df.center)

# Plot scatter chart
df.plot(kind='scatter', x = 'datapoint_x', y = 'datapoint_y', s=0.1, c = cols)
# Set the x and y ticks
x_ticks = np.arange(-74.10, -73.90, 0.05)
y_ticks = np.arange(40.50, 41.00, 0.05)
plt.yticks(y_ticks)
plt.show()