import warnings

warnings.filterwarnings('ignore')  # Hide warnings
import datetime as dt
import pandas as pd

pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from PIL import Image
import os

from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import streamlit as st
import plotly.figure_factory as ff

st.title('Stock Market App')
'---------------------------------------------------------'

com = st.text_input("Enter the Stock Code of company", "AAPL")

st_date = st.text_input("Enter Starting date as YYYY-MM-DD", "2000-01-10")

end_date = st.text_input("Enter Ending date as YYYY-MM-DD", "2000-01-20")

df = web.DataReader(com, 'yahoo', st_date, end_date)  # Collects data
df.reset_index(inplace=True)
df.set_index("Date", inplace=True)

df

ohlc_day = st.text_input("Enter number of days for Resampling for OHLC CandleStick Chart", "50")

df_ohlc = df.Close.resample(ohlc_day + 'D').ohlc()
df_volume = df.Volume.resample(ohlc_day + 'D').sum()

df_ohlc.reset_index(inplace=True)
df_ohlc.Date = df_ohlc.Date.map(mdates.date2num)

# Create and visualize candlestick charts
fig = plt.figure(figsize=(8, 6))

ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
ax1.xaxis_date()
candlestick_ohlc(ax1, df_ohlc.values, width=2, colorup='g')
plt.xlabel('Time')
plt.ylabel('Stock Candle Sticks')
st.pyplot(fig)


x1 = np.random.randn(200)
hist_data = [x1]
group_labels = ['Group 1']
fig = ff.create_distplot(hist_data, group_labels, bin_size=[.1, .25, .5])
st.plotly_chart(fig, use_container_width=True)