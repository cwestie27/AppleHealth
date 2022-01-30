#############All in
import pandas as pd
import xml.etree.ElementTree as ET
import datetime as dt

def manipulatecsv(health_data):
    today = dt.datetime.now().strftime('%Y-%m-%d')
    #health_data= pd.read_csv(r"C:\Users\16158\CWScratch\Health Data\apple_health_export_" + today + ".csv")
    #health_data = health_data
    #health_data.head()


    filtered_health_data = health_data[['creationDate','type', 'value','sourceName']]
    filtered_health_data
    fieldlist = ['StepCount','DietaryProtein','DietaryCarbohydrates','DietaryFatTotal','RestingHeartRate',
                 'BodyMassIndex', 'BodyFatPercentage', 'BasalEnergyBurned','LeanBodyMass']
    dd = filtered_health_data.loc[filtered_health_data['type'].isin(fieldlist)]
    zlm = dd
    dd = dd.drop(dd.index[(dd.sourceName == "carson’s iPhone")&(dd['type']== 'StepCount')])
    dd = dd.drop(dd.index[(dd.sourceName == "FitTrack")&(dd['type']== 'BasalEnergyBurned')])
    dd['creationDate'] = pd.to_datetime(dd['creationDate'])
    dd['value'] = dd['value'].astype(float)
    dd['day'] = dd['creationDate'].dt.floor('D')
    dd['conc'] = dd['day'].astype(str) + dd['type']

    #dd['total'] = df[['value']].sum(axis=1).where(df['type']== 'StepCount')
    group = dd.groupby(['conc'])['value'].sum()
    dd['totals'] = dd['conc'].map(group)  #add the totals column to the unfiltereddataframe
    dd1 = dd.drop_duplicates(subset='day', keep="first")


    xyz = dd.loc[(dd.type == fieldlist[1])] #stepcount
    xyz1 = xyz[['day','totals']]
    ok = pd.merge(dd1, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[2])] #protein
    xyz1 = xyz[['day','totals']]
    ok1 = pd.merge(ok, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[3])] # carbs
    xyz1 = xyz[['day','totals']]
    ok2 = pd.merge(ok1, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[4])] #fat
    xyz1 = xyz[['day','totals']]
    ok3 = pd.merge(ok2, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[5])] #BMI
    xyz1 = xyz[['day','value']]
    ok4 = pd.merge(ok3, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[6])] #BFP
    xyz1 = xyz[['day','value']]
    ok5 = pd.merge(ok4, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[7])] #BasalEnergy
    xyz1 = xyz[['day','totals']]
    ok6 = pd.merge(ok5, xyz1, on = 'day',how = 'left')

    xyz = dd.loc[(dd.type == fieldlist[8])] #LBM
    xyz1 = xyz[['day','value']]
    ok7 = pd.merge(ok6, xyz1, on = 'day',how = 'left')

    ok7 = ok7.drop_duplicates(subset='day', keep="first")
    lmn = ok7
    ok7.columns.values[6] = 'delete'
    for i in range(len(fieldlist)):
    #ok3.rename(columns = {'test':'TEST', 'odi':'ODI','t20':'T20'}, inplace = True)
        ok7.columns.values[i+6] = fieldlist[i]
    ok7=ok7.drop(ok3.columns[[4, 5]], axis=1)

    today = dt.datetime.now().strftime('%Y-%m-%d')
    file_name = "CWLWHealthData"+today+".csv"
    ourperiod = ok7.loc[(ok7['creationDate'] > '2021-10-16') & (ok7['creationDate'] < '2021-11-24')] ############  Select Period

    today = dt.datetime.now().strftime('%Y-%m-%d')
    sleep_data1= pd.read_csv(r"C:\Users\16158\CWScratch\Health Data\sleepdata.csv",sep=";")
    sleep_data = sleep_data1[['Start', 'End','Sleep Quality', 'Regularity','Weather temperature (°F)', 'Weather type','Time in bed (seconds)', 'Time asleep (seconds)',
           'Time before sleep (seconds)','Notes']]
    sleep_data['Start'] = pd.to_datetime(sleep_data['Start'])
    sleep_data['End'] = pd.to_datetime(sleep_data['End'])
    sleep_data['day'] = sleep_data['Start'].dt.floor('D').astype('datetime64[ns]').dt.strftime('%m/%d/%Y')
    sleep_data['Time in bed (seconds)'] = sleep_data['Time in bed (seconds)']/60/60
    sleep_data['Time asleep (seconds)'] = sleep_data['Time asleep (seconds)']/60/60
    sleep_data['Time before sleep (seconds)'] = sleep_data['Time before sleep (seconds)']/60/60
    sleep_data.rename(columns={'Time before sleep (seconds)': 'Time before sleep (hours)', 'Time in bed (seconds)': 'Time in bed (hours)', 'Time asleep (seconds)': 'Time asleep (hours)'}, inplace=True)
    sleep_data
    ourperiod['day'] = ourperiod['creationDate'].dt.floor('D').astype('datetime64[ns]').dt.strftime('%m/%d/%Y')
    ourperiod.set_index('day', drop=True, append=False, inplace=True, verify_integrity=False)
    sleep_data.set_index('day', drop=True, append=False, inplace=True, verify_integrity=False)
    merged = pd.merge(ourperiod, sleep_data, on = 'day',how = 'left')
    merged.drop(['value_x','type'], axis=1, inplace=True)
    merged['Start'] = merged.Start.dt.strftime('%I:%M %p')
    merged['End'] = merged.End.dt.strftime('%I:%M %p')
    merged['creationDate'] = merged.creationDate.dt.strftime('%m/%d/%Y')
    #merged.head()

    merged.to_csv(file_name, encoding='utf-8', index=False)
    return merged
