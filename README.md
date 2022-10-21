```python
import os
import glob
import pandas as pd
import psycopg2
from sql_queries import *
```


```python
conn = psycopg2.connect(host="localhost",dbname="sparkifydb",user="postgres",password="Immortalmk03@")
cur = conn.cursor()
```


```python
def getfiles(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
            
    return all_files
```


```python
song_file = getfiles('./data/song_data')
```


```python
filepath = song_file[0]
```


```python
df = pd.read_json(filepath, lines=True)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>num_songs</th>
      <th>artist_id</th>
      <th>artist_latitude</th>
      <th>artist_longitude</th>
      <th>artist_location</th>
      <th>artist_name</th>
      <th>song_id</th>
      <th>title</th>
      <th>duration</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>ARD7TVE1187B99BFB1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>California - LA</td>
      <td>Casual</td>
      <td>SOMZWCG12A8C13C480</td>
      <td>I Didn't Mean To</td>
      <td>218.93179</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
song_data_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
song_data_values = song_data_df.values
first_record_df = song_data_values[0]
song_data = first_record_df.tolist()
song_data
```




    ['SOMZWCG12A8C13C480', "I Didn't Mean To", 'ARD7TVE1187B99BFB1', 0, 218.93179]




```python
cur.execute(song_table_insert, song_data)
conn.commit()
```


```python
artist_data_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
artist_data_values = artist_data_df.values
first_record_df = artist_data_values[0]
artist_data = first_record_df.tolist()
artist_data
```




    ['ARD7TVE1187B99BFB1', 'Casual', 'California - LA', nan, nan]




```python
cur.execute(artist_table_insert, artist_data)
conn.commit()
```


```python
log_file = getfiles('./data/log_data')
filepath = log_file[0]
```


```python
df = pd.read_json(filepath, lines=True)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>Logged In</td>
      <td>Walter</td>
      <td>M</td>
      <td>0</td>
      <td>Frye</td>
      <td>NaN</td>
      <td>free</td>
      <td>San Francisco-Oakland-Hayward, CA</td>
      <td>GET</td>
      <td>Home</td>
      <td>1540919166796</td>
      <td>38</td>
      <td>None</td>
      <td>200</td>
      <td>1541105830796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>39</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>0</td>
      <td>Summers</td>
      <td>NaN</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>GET</td>
      <td>Home</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>None</td>
      <td>200</td>
      <td>1541106106796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Des'ree</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>1</td>
      <td>Summers</td>
      <td>246.30812</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>You Gotta Be</td>
      <td>200</td>
      <td>1541106106796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>None</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>2</td>
      <td>Summers</td>
      <td>NaN</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>GET</td>
      <td>Upgrade</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>None</td>
      <td>200</td>
      <td>1541106132796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mr Oizo</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>3</td>
      <td>Summers</td>
      <td>144.03873</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>Flat 55</td>
      <td>200</td>
      <td>1541106352796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
  </tbody>
</table>
</div>




```python
df = df[df.page == 'NextSong']
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>Des'ree</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>1</td>
      <td>Summers</td>
      <td>246.30812</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>You Gotta Be</td>
      <td>200</td>
      <td>1541106106796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mr Oizo</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>3</td>
      <td>Summers</td>
      <td>144.03873</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>Flat 55</td>
      <td>200</td>
      <td>1541106352796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Tamba Trio</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>4</td>
      <td>Summers</td>
      <td>177.18812</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>Quem Quiser Encontrar O Amor</td>
      <td>200</td>
      <td>1541106496796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>6</th>
      <td>The Mars Volta</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>5</td>
      <td>Summers</td>
      <td>380.42077</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>Eriatarka</td>
      <td>200</td>
      <td>1541106673796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Infected Mushroom</td>
      <td>Logged In</td>
      <td>Kaylee</td>
      <td>F</td>
      <td>6</td>
      <td>Summers</td>
      <td>440.26730</td>
      <td>free</td>
      <td>Phoenix-Mesa-Scottsdale, AZ</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1540344794796</td>
      <td>139</td>
      <td>Becoming Insane</td>
      <td>200</td>
      <td>1541107053796</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>8</td>
    </tr>
  </tbody>
</table>
</div>




```python
t = pd.to_datetime(df['ts'],unit='ms')
t.head()
```




    2   2018-11-01 21:01:46.796
    4   2018-11-01 21:05:52.796
    5   2018-11-01 21:08:16.796
    6   2018-11-01 21:11:13.796
    7   2018-11-01 21:17:33.796
    Name: ts, dtype: datetime64[ns]




```python
time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
```

    C:\Users\hp\AppData\Local\Temp\ipykernel_11448\3183324291.py:1: FutureWarning: Series.dt.weekofyear and Series.dt.week have been deprecated. Please use Series.dt.isocalendar().week instead.
      time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    


```python
time_dict = dict(zip(column_labels, time_data))
time_df = pd.DataFrame(time_dict)
time_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>hour</th>
      <th>day</th>
      <th>weekofyear</th>
      <th>month</th>
      <th>year</th>
      <th>weekday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>2018-11-01 21:01:46.796</td>
      <td>21</td>
      <td>1</td>
      <td>44</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-01 21:05:52.796</td>
      <td>21</td>
      <td>1</td>
      <td>44</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018-11-01 21:08:16.796</td>
      <td>21</td>
      <td>1</td>
      <td>44</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2018-11-01 21:11:13.796</td>
      <td>21</td>
      <td>1</td>
      <td>44</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2018-11-01 21:17:33.796</td>
      <td>21</td>
      <td>1</td>
      <td>44</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))
    conn.commit()
```


```python
user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
user_df = user_df.drop_duplicates().dropna()
user_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>userId</th>
      <th>firstName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>level</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>8</td>
      <td>Kaylee</td>
      <td>Summers</td>
      <td>F</td>
      <td>free</td>
    </tr>
    <tr>
      <th>10</th>
      <td>10</td>
      <td>Sylvie</td>
      <td>Cruz</td>
      <td>F</td>
      <td>free</td>
    </tr>
    <tr>
      <th>12</th>
      <td>26</td>
      <td>Ryan</td>
      <td>Smith</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>14</th>
      <td>101</td>
      <td>Jayden</td>
      <td>Fox</td>
      <td>M</td>
      <td>free</td>
    </tr>
  </tbody>
</table>
</div>




```python
for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)
    conn.commit()
```


```python
for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()
    
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()
```

