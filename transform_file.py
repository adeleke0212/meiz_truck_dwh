import pandas as pd


def transformCustomers():
    df = pd.read_csv('Datasets/customers.csv')
    df['registered_at'] = pd.to_datetime(df['registered_at'])
    df['ConvertedDate'] = df['registered_at'].astype(str)
    df['registered_year'] = pd.DatetimeIndex(df['registered_at']).year
    df['registered_month'] = pd.DatetimeIndex(df['registered_at']).month
    df['registered_day'] = pd.DatetimeIndex(df['registered_at']).day
    return df


def slicer(datetime_text): return datetime_text[11:13]


def getHour(df):
    df['registered_hour'] = df['ConvertedDate'].apply(slicer)
    df['registered_hour'] = df['registered_hour'].astype(int)
    df = df[['id', 'name', 'email', 'registered_at', 'registered_year',
             'registered_month', 'registered_day', 'registered_hour']]
    df.to_csv('transformed_Datasets/customers2.csv')


df = transformCustomers()
getHour(df)
