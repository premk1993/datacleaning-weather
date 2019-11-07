import pandas as pd
import numpy as np


def mean_calculation(dataframe,mean_on,feature):
    if mean_on=='year':
        mean_years={}
        years=dataframe.yr.unique()
        for i in years:
            mean_years[i]=dataframe.wdsp[dataframe.yr==i].mean(axis=0,skipna=True)
        return mean_years;
    
    if mean_on=='stationid':
        mean_wsids={}
        wsids=dataframe.wsid.unique()
        for i in wsids:
            mean_wsids[i]=dataframe.stp[dataframe.wsid==i].mean(axis=0,skipna=True)
        return mean_wsids;
    
    if mean_on=='stationid_year_month':
        wsids=dataframe.wsid.unique()
        years=dataframe.yr.unique()
        months=dataframe.mo.unique()
        months_dict={}
        year_months={}
        stn_yr_mo={}
        for h in months:
            months_dict[h]=0
        for i in years:
            year_months[i]=months_dict
        
        for j in wsids:
            stn_yr_mo[j]=year_months
        
        for k in stn_yr_mo:
            for l in stn_yr_mo[k]:
                for m in stn_yr_mo[k][l]:
                    if feature=='temp':
                        stn_yr_mo[k][l][m]=dataframe.temp[(dataframe.wsid==k)&(dataframe.yr==l)&(dataframe.mo==m)].mean(axis=0,skipna=True)
                    if feature=='dewp':
                        stn_yr_mo[k][l][m]=dataframe.dewp[(dataframe.wsid==k)&(dataframe.yr==l)&(dataframe.mo==m)].mean(axis=0,skipna=True)
        #writing the dictionary to file
        import json
        json = json.dumps(str(stn_yr_mo))
        f = open(feature+"_dictionary.json","w")
        f.write(json)
        f.close()
        
        return stn_yr_mo;
            
        

     
def wdsp(df1,feature):
    mean_years=mean_calculation(df1,mean_on='year',feature=feature)
    if feature=='gust':
        dfwdsp=df1[df1.gust>0]
    
    if feature=='wdct':
        dfwdsp=df1[df1.wdct>0]
        
    dfwdsp["wdsp_meanflag"]=""
    
    for i in mean_years:
        dfwdsp.wdsp[((dfwdsp.wdsp==0) |(dfwdsp.wdsp.isna()) ) & (dfwdsp.yr==i)]=mean_years[i]
        dfwdsp.wdsp_meanflag[dfwdsp.wdsp==mean_years[i]]=True                
    return dfwdsp;

def wind(dataframe_master):
    dataframe_master.wdsp[dataframe_master.wdsp==0]=dataframe_master.wdsp[dataframe_master.wdsp>0].mean(axis=0,skipna=True)
    dataframe_master.wdct[dataframe_master.wdct==0]=dataframe_master.wdct[dataframe_master.wdct>0].mean(axis=0,skipna=True)
    dataframe_master.gust[dataframe_master.gust==0]=dataframe_master.gust[dataframe_master.gust>0].mean(axis=0,skipna=True)
    return dataframe_master;

def stp_cleaning(dataframe_master,feature):
    #upon dataframe analysis any value below 877.3 and above 1030.3 are outliers as air pressure cannot be 0 on earth. Hence we replace the outliers with means of stp wrt to the stationIDs as the air pressure varies based on the altitude.
    mean_wsids=mean_calculation(dataframe_master,mean_on='stationid',feature=feature)
    for i in mean_wsids:
        dataframe_master.stp[(dataframe_master.wsid==i) & ((dataframe_master.stp<877.3) |(dataframe_master.stp>1030.3))]=mean_wsids[i]    
    dataframe_stp=dataframe_master
    return dataframe_stp;

def temp_cleaning(dataframe_master,feature):
    stn_yr_mo=mean_calculation(dataframe_master,mean_on='stationid_year_month',feature=feature)
    for k in stn_yr_mo:
            for l in stn_yr_mo[k]:
                for m in stn_yr_mo[k][l]:
                    dataframe_master.temp[((dataframe_master.temp==0)|(dataframe_master.temp==999))&(dataframe_master.wsid==k)&(dataframe_master.yr==l)&(dataframe_master.mo==m)]=stn_yr_mo[k][l][m]
    
    #for wsid where the mean for the records turns out to be 0 because the error is computed for the whole month and the dta has 0 we are replacing with complete mean.
    dataframe_master.temp[(dataframe_master.temp==0)|(dataframe_master.temp.isna())]=dataframe_master.temp.mean(axis=0,skipna=True)
    return dataframe_master;    
    

def dewp_cleaning(dataframe_master,feature):
    
    stn_yr_mo=mean_calculation(dataframe_master,mean_on='stationid_year_month',feature=feature)
    for k in stn_yr_mo:
            for l in stn_yr_mo[k]:
                for m in stn_yr_mo[k][l]:
                    dataframe_master.dewp[((dataframe_master.dewp==0)|(dataframe_master.dewp.isna()))&(dataframe_master.wsid==k)&(dataframe_master.yr==l)&(dataframe_master.mo==m)]=stn_yr_mo[k][l][m]
    
    
    dataframe_master.dewp[((dataframe_master.dewp==0)|(dataframe_master.dewp.isna()))]=dataframe_master.dewp.mean(axis=0,skipna=True)
    return dataframe_master;

def hmdy_cleaning(dataframe_master,feature):
    dataframe_master.hmdy[dataframe_master.hmdy==0]=dataframe_master.hmdy.mean(axis=0,skipna=True)
    return dataframe_master;



####------Above functions are defined--------######

datalog = pd.read_csv("WeatherData_V2.csv")

##### Replace city data with modes ########################
modes = pd.DataFrame(columns = ['wsid', 'lat', 'lon', 'inme', 'city', 'prov'])
modes['wsid'] = datalog.wsid.unique()
#print(datalog['prcp'].value_counts())
for id in modes['wsid']:
    for column in modes:
        modes.loc[modes['wsid'] == id, column] = datalog.loc[datalog['wsid'] == id].mode()[column][0]

for i in modes['wsid']:
    datalog.loc[datalog.wsid == i, 'lat'] = float(modes.loc[modes['wsid'] == i, 'lat'])
    datalog.loc[datalog.wsid == i, 'lon'] = float(modes.loc[modes['wsid'] == i, 'lon'])
    datalog.loc[datalog.wsid == i, 'inme'] = modes.loc[modes['wsid'] == i, 'inme'].to_string()
    datalog.loc[datalog.wsid == i, 'city'] = modes.loc[modes['wsid'] == i, 'city'].to_string()
    datalog.loc[datalog.wsid == i, 'prov'] = modes.loc[modes['wsid'] == i, 'prov'].to_string()
############################################################

#feature: Windspeed (wdsp)
#Handling windspeed based on wind gust, ie. if gust avaialble we have taken mean of windspeed based on year and imputed in wdsp.
df2=wdsp(datalog,'gust')
datalog.loc[df2[df2.wdsp_meanflag==True].index]=df2[df2.wdsp_meanflag==True]

#Handling windspeed based on wind direction (wdct), ie. if wdct avaialble we have taken mean of windspeed based on year and imputed in wdsp.
df2=wdsp(datalog,'wdct')
datalog.loc[df2[df2.wdsp_meanflag==True].index]=df2[df2.wdsp_meanflag==True]
datalog=wind(datalog)

#feauture: Air Pressure(stp)
#the stp feature's cleaning his done in the stp_cleaning function, we impute the mean in all the rows that are 0s, null or outliers. The mean is calculated per station as the stp varies based on the Altitude and replaced respectively in the column values. 0 is replaced in the similar fashion because the stp cannot be 0 on earths atmosphere.
datalog=stp_cleaning(datalog,'stp')

#feature: Air Temperature(temp)
#
datalog=temp_cleaning(datalog,'temp')


#feature: Dew PointTemperature (dewp)
# the dewp can be in -ve and +ve values, hence only the null/NA values have been replaced with the mean.
datalog=dewp_cleaning(datalog,'dewp')

#feature: Relative Humidity(hmdy)
# the hmdy cannot be 0 as it is immpossible for humidity to be 0 on earth surface, Water vapor is always present in the air, even if only in minute quantities. Hence we take a mean an impute in the column whereever there is 0 humidity
datalog=hmdy_cleaning(datalog,'hmdy')

#### Replace 0's and anomalies in precipitation #############################
for i in range(len(datalog.index)):
    if (datalog.loc[i,'stp'] != 0) & (datalog.loc[i,'temp'] != 0) & (datalog.loc[i,'dewp'] != 0) & ((datalog.loc[i,'prcp'] == 0) | (datalog.loc[i,'prcp'] > 60)):
        avg = (datalog.loc[i-1,'prcp'] + datalog.loc[i+1,'prcp']) / 2
        datalog.loc[i,'prcp'] = avg

datalog = datalog.replace(to_replace=0,value=np.NaN)
################################################################################

#### Replace extreme temperature changes ########################################
for i in range(1,len(datalog.index)):
    dif = abs(float(datalog.loc[i,'temp']) - float(datalog.loc[i-1,'temp']))
    if dif >= 20:
        avg = (datalog.loc[i-1,'temp'] + datalog.loc[i+1,'temp']) / 2
        datalog.loc[i,'temp'] = avg

datalog.to_csv("Final_data_cleaned.csv")
###################################################################################