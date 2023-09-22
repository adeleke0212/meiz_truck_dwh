import pandas as pd


def transformCustomers():
    df = pd.read_csv('Datasets/customers.csv')
    df['registered_at'] = pd.to_datetime(df['registered_at'])
    df['ConvertedDate'] = df['registered_at'].astype(str)
    df['registered_year'] = pd.DatetimeIndex(df['registered_at']).year
    df['registered_month'] = pd.DatetimeIndex(df['registered_at']).month
    df['registered_day'] = pd.DatetimeIndex(df['registered_at']).day
    return df


def transformCustomers():
    df = pd.read_csv('Datasets/customers.csv')
    df['registered_at'] = pd.to_datetime(df['registered_at'])
    df['ConvertedDate'] = df['registered_at'].astype(str)
    df['registered_year'] = pd.DatetimeIndex(df['registered_at']).year
    df['registered_month'] = pd.DatetimeIndex(df['registered_at']).month
    df['registered_day'] = pd.DatetimeIndex(df['registered_at']).day
    return df

# slicer = lambda datetime_text: datetime_text[11:13]


def slicer(datetime_text): return datetime_text[11:13]


def getHour(df):
    df['registered_hour'] = df['ConvertedDate'].apply(slicer)
    df['registered_hour'] = df['registered_hour'].astype(int)
    df = df[['id', 'name', 'email', 'registered_at', 'registered_year',
             'registered_month', 'registered_day', 'registered_hour']]
    df.to_csv('transformed_Datasets/customerstdf.csv')


def getHour(df):
    df['registered_hour'] = df['ConvertedDate'].apply(slicer)
    df['registered_hour'] = df['registered_hour'].astype(int)
    df = df[['id', 'name', 'email', 'registered_at', 'registered_year',
             'registered_month', 'registered_day', 'registered_hour']]
    df.to_csv('transformed_Datasets/customers.csv')


df = transformCustomers()
getHour(df)

# transform transactions.csv


def transformTransactions():
    tdf = pd.read_csv('Datasets/transactions.csv')
    tdf['date'] = pd.to_datetime(tdf['date'])
    tdf['transaction_year'] = pd.DatetimeIndex(tdf['date']).year
    tdf['transaction_month'] = pd.DatetimeIndex(tdf['date']).month
    tdf['transaction_day'] = pd.DatetimeIndex(tdf['date']).day
    tdf["quarter"] = tdf.date.dt.quarter
    tdf.to_csv('transformed_Datasets/transactions.csv')


transformTransactions()