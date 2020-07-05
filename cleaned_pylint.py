import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def clean(week_nums):
    '''
    Takes in a list of weeks, returns a dataframe with the CSV's combined and time 
    
    ~~~~
    Parameters
    ----
    week_nums : TYPE = list (int)

    Returns
    ----
    dfs : TYPE = dataframe
    
    '''	
    dfs = []
    for week_num in week_nums:
        url = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_{}.txt"
        file_url = url.format(week_num)
        names = ['c_a', 'unit', 'scp', 'station', 'linename', 'division', 
                 'date', 'time', 'desc', 'entries', 'exits']
        df = pd.read_csv(file_url, names=names, parse_dates=[['date','time']], 
                         keep_date_col=True, skiprows=1)
        df['date'] = pd.to_datetime(df['date'])
        df['day_of_week'] = df['date_time'].dt.day_name()
        df = df[(~df['c_a'].str.contains('PTH') & 
                 ~df['desc'].str.contains('RECOVR') & 
                 df.time.astype(str).str.contains('00:00'))]
        df = df[['station', 'unit', 'c_a', 'scp', 'date_time', 'date', 'day_of_week', 'time', 
                'desc', 'entries', 'exits']]
        #delete duplicates -> could be done after dataframe creation instead of in this loop
        df.sort_values(['c_a', 'unit', 'scp', 'station', 'date_time'], inplace=True, ascending=False)
        df.drop_duplicates(subset=['c_a', 'unit', 'scp', 'station', 'date_time'], inplace=True)
        dfs.append(df)
        print(week_num)
    return pd.concat(dfs)

week_nums = [190622]

turnstiles_df = clean(week_nums)

#add column specifying the prior block of time to find the change in total entries / exits
'''
This code creates groups based on SCP (physical turnstile lanes).
'''
turnstiles_block = (turnstiles_df
                        .groupby(["c_a", "unit", "scp", "station", "date_time", "exits"],as_index=False).entries.first())
turnstiles_block[["prev_datetime", "prev_entries", "prev_exits"]] = (turnstiles_block
                                                       .groupby(["c_a", "unit", "scp", "station"])["date_time", "entries", "exits"]
                                                       .apply(lambda grp: grp.shift(1)))

turnstiles_block.dropna(subset=["prev_datetime"], axis=0, inplace=True)

def get_counts_entry(row, max_counter):
    '''
    When called, takes in a row number and a max counter value and applies a test to see if the counter makes sense, then returns the delta between entries.
    
    ~~~~
    Parameters
    ----
    row : TYPE = int (row number in a dataframe)
    max_counter : TYPE = int

    Returns
    ----
    counter : TYPE = int
    
    '''
    counter = row["entries"] - row["prev_entries"]
    if counter < 0:
        # Maybe counter is reversed?
        counter = -counter
    if counter > max_counter:
        # Maybe counter was reset to 0? 
        print(row["entries"], row["prev_entries"])
        counter = min(row["entries"], row["prev_entries"])
    if counter > max_counter:
        # Check it again to make sure we're not still giving a counter that's too big
        return 0
    return counter

def get_counts_exit(row, max_counter):
    '''
    When called, takes in a row number and a max counter value and applies a test to see if the counter makes sense, then returns the delta between exits.
    
    ~~~~
    Parameters
    ----
    row : TYPE = int (row number in a dataframe)
    max_counter : TYPE = int

    Returns
    ----
    counter : TYPE = int
    
    '''
    counter = row["exits"] - row["prev_exits"]
    if counter < 0:
        # Maybe counter is reversed?
        counter = -counter
    if counter > max_counter:
        # Maybe counter was reset to 0? 
        print(row["exits"], row["prev_exits"])
        counter = min(row["exits"], row["prev_exits"])
    if counter > max_counter:
        # Check it again to make sure we're not still giving a counter that's too big
        return 0
    return counter

#filter outlier exit and entry counts
'''
It is thought that it takes a minimum of 5 seconds to go through a turnstile. This means 2880 theoretical max input from an SCP in a 4 hour period. Increasing this by a factor of 5 gives us 14400, or about 15000
'''
turnstiles_block['delta_entries'] = turnstiles_block.apply(get_counts_entry, axis=1, max_counter=15000) #30000 max_counter value based on an order of magnitude larger than the highest possible values for SCP.
turnstiles_block['delta_exits'] = turnstiles_block.apply(get_counts_exit, axis=1, max_counter=15000)


#group turnstiles by time block, total
'''
This code sums turnstiles into Units (Remote units), or physical station locations based on billing. It keeps different names of stations still separate.
'''
unit_hourly = (turnstiles_block.groupby(['station','unit','date_time'])['delta_exits','delta_entries'].sum().reset_index())

#Dataframe that sums exit/entry counts count by station / time blocks overall (even with stations that have 2 different names)
top = unit_hourly.groupby(['unit', 'date_time'])['delta_exits','delta_entries'].sum().reset_index()

#head length variable-> visually assess list til 10 uniques
top10 = (top.sort_values(by=['delta_exits'],ascending=False)
        .unit.head(100))

plt.figure(figsize=[15,10])
for i in top5units: 
    mask = (top['unit'] == i)
    top5_df = top[mask]
    plt.plot(top5_df.date_time, top5_df.delta_exits, label = top5_df.unit.iloc[0])
plt.legend()

plt.title('Traffic in the top 5 stations during COVID-19', fontsize=20, weight='bold',color='b')
plt.xlabel('Date', fontsize=15, weight='bold',color='b')
plt.ylabel('Traffic', fontsize=15, weight='bold',color='b')
plt.savefig('top5 stations traffic.png')

plt.figure(figsize=[15,10])
plt.plot(sample_df.date_time, sample_df.delta_exits)

for i in top10units:
    mask = (unit_hourly['unit'] == i)
    top5_df = unit_hourly[mask].head(50)

    plt.plot(top5_df.date_time, top5_df.delta_exits, label = unit_hourly['unit' ==i]
plt.legend()