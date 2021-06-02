import sqlalchemy
import pandas as pd

def lastupdated(engine,logger):
    import datetime
    df = pd.read_sql_table('vinpacCleaned', con=engine)
    ndf = pd.DataFrame([[df['t_stamp'].min(), df['t_stamp'].max(), datetime.datetime.now()]], columns=['Start_Date', 'End_Date', 'Updated_Date'])
    ndf.to_sql('UpdatedDetails',con = engine, if_exists='replace', index=False)
    del df, ndf

def GetLastUpdatedMsg(engine,logger):
    import datetime
    try:
        df = pd.read_sql_table('UpdatedDetails', con=engine)
        msg = "File was last uploaded on "+ df['Updated_Date'].iloc[0].strftime('%Y-%m-%d %H:%M:%S') +". It consists of data from "+ df['Start_Date'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')   + " to " + df['End_Date'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')   + "."
    except:
        msg = "Please upload file to see the results!!"

    return msg

def changedstatus(engine, logger):
    df = pd.read_sql_table('vinpacCleaned', con=engine)
    df = df.sort_values(by='t_stamp') 
    dfMachines = pd.DataFrame()
    machines = df.columns
    machines = machines.drop('t_stamp')
    for machine in machines:    
        log_str = "Updating " + machine + " Status "
        logger.info(log_str)
        dfMachine = df.loc[:, ('t_stamp',machine)]
        dfMachineSC = dfMachine.loc[dfMachine[machine] != dfMachine[machine].shift(1)]
        dfMachineSC['duration'] = dfMachineSC['t_stamp'].shift(-1) - dfMachineSC['t_stamp']
        dfMachineSC['duration_sec'] = dfMachineSC['duration'].astype('timedelta64[ms]')/1000
        dfMachineSC['Machine'] = machine
        dfMachineSC.rename(columns={machine:'Status'}, inplace=True)
        dfMachines = dfMachines.append(dfMachineSC, ignore_index=True)    
    
    statusMapValues = {0:'Running', 1:'Safety Stopped', 2:'Starved', 3:'Blocked', 4:'Faulted', 5:'Unallocated', 6:'User Stopped', 7:'Off', 8:'Setup' , 9:'Runout', 10:'Idle'}
    dfMachines['Status'] = dfMachines['Status'].map(statusMapValues)
    def prepData(df):
        #df = dfMachines
        df['duration_hr'] = df['duration_sec']/3600
        df = df.groupby(['Machine','Status']).agg({'t_stamp':'count', 'duration_hr': 'sum'}).reset_index()
        df.columns = ['Machine','Status','Count','Duration_Hours']

        eff_states = ['Safety Stopped','Starved','Blocked','Faulted','Unallocated','User Stopped','Running']
        df = df.loc[df['Status'].isin(eff_states)]
        df['Percent']  = (df['Duration_Hours'] / df['Duration_Hours'].sum()) * 100
        
        return df
    machStopChange = pd.DataFrame()
    machine_names = dfMachines.Machine.unique()
    for machine in machine_names:
        dfm = dfMachines.loc[dfMachines.Machine == machine]
        machStopChange = machStopChange.append(prepData(dfm))
    #df1 = prepData(dfMachines)

    dfMachines.to_sql('Machine_Status_Change',con = engine, if_exists='replace', index=False)
    machStopChange.to_sql('MachineStoppageChange',con = engine, if_exists='replace', index=False)

    del dfMachines, df, machStopChange


def getInbetStopDet(filler_status_df, cleaned, filler_status):
    df_grouped = pd.DataFrame()
    for index, row in filler_status_df.iterrows():
        tdf = cleaned.loc[(cleaned.t_stamp >= row['Start_Time']) & (cleaned.t_stamp <= row['End_Time'])]
        tdf_time = (tdf['t_stamp'].iloc[-1] - tdf['t_stamp'].iloc[0]).total_seconds()
        #tdf_grouped = pd.newDataFrame()
        for column in cleaned.columns:
            if str(column) not in ['t_stamp', 'Filler']:
                dfMachineSC = tdf.loc[tdf[column] != tdf[column].shift(1)]
                if dfMachineSC.size == 1:
                    dfMachineSC['duration_sec'] = tdf_time
                else:
                    dfMachineSC['duration_sec'] = (dfMachineSC['t_stamp'].shift(-1) - dfMachineSC['t_stamp']).astype('timedelta64[ms]')/1000
                    dur = tdf_time - dfMachineSC['duration_sec'].sum()
                    dfMachineSC['duration_sec'].fillna(dur, inplace=True)
                tdf_grouped = dfMachineSC.groupby(column).agg({column:'count', 'duration_sec':'sum'}).rename(columns={column:'Count'}).reset_index().rename(columns={column:'Status'})                
                tdf_grouped['Machine'] = column
                tdf_grouped['Start_Time'] = row['Start_Time']
                tdf_grouped['End_Time'] = row['End_Time']
                df_grouped = df_grouped.append(tdf_grouped, ignore_index=True)
    df_grouped['Filler_Status'] = filler_status
    return df_grouped

def machineDetailsFillerStop(engine, logger):
    logger.info("Calculation machine stoppages when filler stopped")
    filler = pd.read_sql_table('Machine_Status_Change', con=engine)
    filler = filler.loc[filler.Machine == 'Filler']
    cleaned = pd.read_sql_table('vinpacCleaned', con=engine)
    dfn = pd.DataFrame()
    #Filler Safety Stopped
    logger.info("filler safety stopped")
    filler_0_1 = pd.DataFrame()
    filler_0_1['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'Safety Stopped')]['t_stamp'].reset_index(drop=True)
    filler_0_1['End_Time'] = filler[(filler['Status'] == 'Safety Stopped') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'Safety Stopped'
    dfn = dfn.append(getInbetStopDet(filler_0_1, cleaned, fstatus),ignore_index=True)
    del filler_0_1
    #Filler Starved
    logger.info("filler starved stopped")
    filler_0_2 = pd.DataFrame()
    filler_0_2['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'Starved')]['t_stamp'].reset_index(drop=True)
    filler_0_2['End_Time'] = filler[(filler['Status'] == 'Starved') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'Starved'
    dfn = dfn.append(getInbetStopDet(filler_0_2, cleaned, fstatus),ignore_index=True)
    del filler_0_2
    #Filler Blocked
    logger.info("filler blocked stopped")
    filler_0_3 = pd.DataFrame()
    filler_0_3['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'Blocked')]['t_stamp'].reset_index(drop=True)
    filler_0_3['End_Time'] = filler[(filler['Status'] == 'Blocked') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'Blocked'
    dfn = dfn.append(getInbetStopDet(filler_0_3, cleaned, fstatus),ignore_index=True)
    del filler_0_3
    #Filler Faulted
    logger.info("filler faulted stopped")
    filler_0_4 = pd.DataFrame()
    filler_0_4['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'Faulted')]['t_stamp'].reset_index(drop=True)
    filler_0_4['End_Time'] = filler[(filler['Status'] == 'Faulted') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'Faulted'
    dfn = dfn.append(getInbetStopDet(filler_0_4, cleaned, fstatus),ignore_index=True)
    del filler_0_4
    #Filler Unallocated Stopped
    logger.info("filler unallocated stopped")
    filler_0_5 = pd.DataFrame()
    filler_0_5['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'Unallocated')]['t_stamp'].reset_index(drop=True)
    filler_0_5['End_Time'] = filler[(filler['Status'] == 'Unallocated') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'Unallocated'
    dfn = dfn.append(getInbetStopDet(filler_0_5, cleaned, fstatus),ignore_index=True)
    del filler_0_5
    #Filler User Stopped
    logger.info("filler user stopped")
    filler_0_6 = pd.DataFrame()
    filler_0_6['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1) == 'User Stopped')]['t_stamp'].reset_index(drop=True)
    filler_0_6['End_Time'] = filler[(filler['Status'] == 'User Stopped') & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)
    fstatus = 'User Stopped'
    dfn = dfn.append(getInbetStopDet(filler_0_6, cleaned, fstatus),ignore_index=True)
    del filler_0_6  

    dfn['duration_sec']= round(dfn['duration_sec'],3)
    statusMapValues = {0:'Running', 1:'Safety Stopped', 2:'Starved', 3:'Blocked', 4:'Faulted', 5:'Unallocated', 6:'User Stopped', 7:'Off', 8:'Setup' , 9:'Runout', 10:'Idle'}
    dfn['Status'] = dfn['Status'].map(statusMapValues)

    grp_dfn = dfn.groupby(['Filler_Status', 'Machine', dfn.Start_Time.dt.date, 'Status']).agg({'Count':'sum', 'duration_sec':'sum'}).reset_index()
    grp_dfn['avg_duration_sec'] = round(grp_dfn['duration_sec']/grp_dfn['Count'],3)
    grp_dfn['duration_sec'] = round(grp_dfn['duration_sec'],3)

    def preparelinedata(df_subset, machine, fstatus):
        linedata = pd.DataFrame()
        statuses = df_subset.Status.unique()
        for status in statuses:
            d = df_subset.loc[df_subset['Status']==status]
            d['Start_Time'] = pd.to_datetime(d['Start_Time']).dt.to_period('D')
            d.set_index('Start_Time', inplace=True)
            idx = pd.period_range(min(grp_dfn.Start_Time), max(grp_dfn.Start_Time)).rename('Start_Time')
            d = d.reindex(idx, fill_value=0)
            d['Status'] = status
            d['Machine'] = machine
            d['Filler_Status'] = fstatus
            d.reset_index(inplace=True)
            linedata = linedata.append(d, ignore_index=True)
        return linedata 

    filler_status = grp_dfn['Filler_Status'].unique()

    linedf = pd.DataFrame()
    for fstatus in filler_status:
        ndf = grp_dfn.loc[grp_dfn.Filler_Status==fstatus]
        machines = ndf['Machine'].unique()
        for machine in machines:
            machinedf = ndf.loc[ndf.Machine == machine]
            linedf = linedf.append(preparelinedata(machinedf, machine, fstatus))
        
    linedf['Start_Time']=pd.PeriodIndex(linedf['Start_Time'], freq='D').to_timestamp()
    linedf['Start_Time'] = pd.to_datetime(linedf['Start_Time'])

    dfn.to_sql('MachineDetailsFillerStoppage',con = engine, if_exists='replace', index=False)
    grp_dfn.to_sql('MachDetFillerStoppageEachDay',con = engine, if_exists='replace', index=False)
    linedf.to_sql('MachStoppageforFillerAllDays',con = engine, if_exists='replace', index=False)

    del dfn, grp_dfn, filler, cleaned, fstatus, linedf

""" def mbaAllZeros(engine, logger):
    df = pd.read_sql_table('vinpacCleaned', con=engine)
    logger.info("Preparing All zeros data for MBA")

    stopped_state_yes = [1,2,3,4,5,6]
    stopped_state_no = 0

    def getZeroIndexes(df):
        #find indexes of records with all machines not in stopped state. 
        idxs = df[(df['Filler']== stopped_state_no) & (df['Depal']== stopped_state_no) & (df['Screwcap']== stopped_state_no) & (df['Dynac']== stopped_state_no) & (df['Labeller']== stopped_state_no) & (df['Packer']== stopped_state_no) & (df['Divider']== stopped_state_no) & (df['Erector']== stopped_state_no) & (df['TopSealer']== stopped_state_no) & (df['Palletiser']== stopped_state_no)].index
        #adding the last index 
        idxs.append(df.tail(1).index)
        return idxs
        
    zeroIdxs = getZeroIndexes(df)

    cntr = 0

    df_groups = []
    item_no = []
    items_combined = [] 
        
    for index, elem in enumerate(zeroIdxs[:-1]):
        for j in range(elem,zeroIdxs[index+1]):
            listmachines = []
            if df.iloc[j]['Filler'] in stopped_state_yes:
                temp_df = df.iloc[elem:j+1]
                #df_groups.append(temp_df)
                bool_tdf = temp_df.shift(1) != temp_df
                for row in range(1,bool_tdf.shape[0]): # df is the DataFrame
                    st = ''
                    for col in range(1,bool_tdf.shape[1]):
                        if bool_tdf.iat[row,col] == True and temp_df.iat[row,col] not in [0,9]:
                            if st=='':
                                st = st+temp_df.columns[col]+"_"+str(temp_df.iat[row , col])    
                            else:
                                st = st+"_"+temp_df.columns[col]+"_"+str(temp_df.iat[row , col])
                    if st!='' :
                        listmachines.append(st)
                cntr+=1
                item_no.append("Stop_"+str(cntr))
                items_combined.append(listmachines)
                break                
            else:
                continue

    groupedData = pd.DataFrame()     
    groupedData['S_no'] = item_no
    groupedData['items'] = items_combined

    def transform_data(df):
        # df = df.drop(df.columns[], axis=1)
        ndf = pd.DataFrame()
        ndf['Item_No'] = df['S_no']   
        for i, row in df.iterrows():
            #iterate the list of items in a group
            for j in row[1]:
                ndf.loc[i,j] = 1
        ndf.fillna(0, inplace=True)
        return ndf

    data = transform_data(groupedData)
    data.to_sql('MBA_All_Zeros',con = engine, if_exists='replace', index=False)
    logger.info("All zero data prepared") """


def mbaFillerChange(engine, logger):
    logger.info("MBA filler change data is being prepared")

    filler = pd.read_sql_table('Machine_Status_Change', con=engine)

    filler = filler.loc[filler.Machine == 'Filler']
    filler['Status'].replace('Safety Stopped','Safety_Stopped', inplace=True)
    filler['Status'].replace('User Stopped','User_Stopped', inplace=True)
    cleaned = pd.read_sql_table('vinpacCleaned', con=engine)
    for column in cleaned.columns:
        if(column != 't_stamp'):
            statusMapValues = {0:'Running', 1:'Safety_Stopped', 2:'Starved', 3:'Blocked', 4:'Faulted', 5:'Unallocated', 6:'User_Stopped', 7:'Off', 8:'Setup' , 9:'Runout', 10:'Idle'}
            cleaned[column] = cleaned[column].map(statusMapValues)

    stopped_states = ['Safety_Stopped','Starved','Blocked','Faulted','Unallocated','User_Stopped']
    filler_times = pd.DataFrame()
    # filler_times['Start_Time'] = filler[filler['Filler']==0 & filler['Filler'].shift(-1).isin([1,2,3,4,5,6])]['t_stamp'].reset_index(drop=True)
    # filler_times['End_Time'] = filler[filler['Filler'].isin([1,2,3,4,5,6]) & filler['Filler'].shift(1) == 0]['t_stamp'].reset_index(drop=True)
    filler_times['Start_Time'] = filler[(filler['Status'] == 'Running') & (filler['Status'].shift(-1).isin(stopped_states))]['t_stamp'].reset_index(drop=True)
    filler_times['End_Time'] = filler[(filler['Status'].isin(stopped_states)) & (filler['Status'].shift(1) == 'Running')]['t_stamp'].reset_index(drop=True)

    cntr = 0

    df_groups = []
    item_no = []
    items_combined = [] 

    for index, row in filler_times.iterrows():
        temp_df = cleaned.loc[(cleaned.t_stamp >= row['Start_Time']) & (cleaned.t_stamp <= row['End_Time'])]
        bool_tdf = temp_df.shift(1) != temp_df
        listmachines = []
        for row in range(1,bool_tdf.shape[0]): # df is the DataFrame
            st = ''
            for col in range(1,bool_tdf.shape[1]):
                if bool_tdf.iat[row,col] == True and temp_df.iat[row,col] not in ['Running','Runout']:
                    if st=='':
                        st = st+temp_df.columns[col]+"_"+str(temp_df.iat[row , col])    
                    else:
                        st = st+"_"+temp_df.columns[col]+"_"+str(temp_df.iat[row , col])
            if st!='' :
                listmachines.append(st)
        cntr+=1
        item_no.append("Stop_"+str(cntr))
        items_combined.append(listmachines)

    groupedData = pd.DataFrame()     
    groupedData['S_no'] = item_no
    groupedData['items'] = items_combined

    def transform_data(df):
        # df = df.drop(df.columns[], axis=1)
        ndf = pd.DataFrame()
        ndf['Item_No'] = df['S_no']   
        for i, row in df.iterrows():
            #iterate the list of items in a group
            for j in row[1]:
                ndf.loc[i,j] = 1
        ndf.fillna(0, inplace=True)
        return ndf

    data = transform_data(groupedData)
    data.to_sql('MBA_Filler_Changed',con = engine, if_exists='replace', index=False)
    logger.info("Filler changa mba data is prepared")


