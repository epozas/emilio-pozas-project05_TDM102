import pandas as pd
from pathlib import Path

def avg_aggreg_temp(file_location , column_title_list, start_date, end_date, element_code = 'TAVG'):
    """
    avg_aggreg_temp is a function that accepts file location, comlumn title list, start and end date, and element code
    It returns the avg value for the rows with that element_code.
    """
    total = 0
    count = 0
    for myDF in pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/{file_location}"), header=None, names=column_title_list,chunksize=10000):
        chunk = myDF[(myDF['date'] >= start_date) & (myDF['date'] <= end_date) & (myDF['element_code'] == element_code)]
        total += sum(chunk['value'])
        count += len(chunk)
    return total / count

avg_aggreg_temp("2018.csv",["id","date","element_code","value","mflag","qflag","sflag","obstime"],20180101,20180115) 

def years_dict(years, column_title_list, element_code):
    mydict= {}
    for myyear in years:
        total = 0
        count = 0
        for myDF in pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/{myyear}.csv"), header=None, names=column_title_list,chunksize=10000):
            chunk = myDF[(myDF['element_code'] == element_code)]
            total += sum(chunk['value'])
            count += len(chunk)
        mydict[myyear] = total/count
    return mydict

years_dict(range(1880,1884),["id","date","element_code","value","mflag","qflag","sflag","obstime"],"TAVG")

def years_to_month_dict(years,mymonth, column_title_list, element_code):
    mydict= {}
    for myyear in years:
        total = 0
        count = 0
        for myDF in pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/{myyear}.csv"), header=None, names=column_title_list,chunksize=10000):
            myDF['month'] = pd.to_datetime(myDF['date'],format='%Y%m%d').dt.month
            chunk = myDF[(myDF['element_code'] == element_code) & (myDF['month']==mymonth)]
            total += sum(chunk['value'])
            count += len(chunk)
        mydict[myyear] = total/count
    return mydict

years_to_month_dict(range(1880,1884),8,["id","date","element_code","value","mflag","qflag","sflag","obstime"],"TAVG")

def max_qflags (years, qflags, column_title_list):
    top_year_count = 0
    top_year = 0
    for myyear in years:
        count = 0
        for myDF in pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/{myyear}.csv"), header=None, names=column_title_list,chunksize=10000):
            chunk = myDF[(myDF['qflag'] == qflags)]
            count += len(chunk)
        if count > top_year_count:
            top_year_count = count
            top_year = myyear
    return top_year

max_qflags(range(1880,1884), "D", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "G", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "I", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "K", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "L", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "N", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "O", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "S", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])
max_qflags(range(1880,1884), "X", ["id","date","element_code","value","mflag","qflag","sflag","obstime"])

myDF= pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/1880.csv"), header=None, names=["id","date","element_code","value","mflag","qflag","sflag","obstime"])
myDF.head
def max_element_code_average (years, element_code, column_title_list =["id","date","element_code","value","mflag","qflag","sflag","obstime"]):
    """
    This function takes a range of years, the element code as well as a column title list. It then goes through the years getting their averages for the selected 
    element code. It keeps the year with the highest average.
    
    args: 
        years(int): The years from the dataframe.
        element_code(str): The chosen element_code from the dataframe
        column_title_list: standarization of the dataframe as it does not have a header. 
        
    Returns:
        int: Returns the year with the highest value average for the selected element code.
    """
    top_year_value = 0
    top_year = 0
    for myyear in years:
        total = 0
        count = 0
        for myDF in pd.read_csv(Path(f"/anvil/projects/tdm/data/noaa/{myyear}.csv"), header=None, names=column_title_list,chunksize=10000):
            chunk = myDF[(myDF['element_code'] == element_code)]
            total += sum(chunk['value'])
            count += len(chunk)
            average = total/count
        if average > top_year_value:
            top_year_value = average
            top_year = myyear
    return top_year
max_element_code_average(range(1880, 1900), 'PRCP')