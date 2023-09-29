import pandas as pd


def transformCustomers():
    df = pd.read_csv('Datasets/customers.csv')
    df['ConvertedDate'] = pd.to_datetime(df['registered_at'])
    df['ConvertedDate'] = df['registered_at'].astype(str)
    df['registered_year'] = pd.DatetimeIndex(df['registered_at']).year
    df['registered_month'] = pd.DatetimeIndex(df['registered_at']).month
    df['registered_day'] = pd.DatetimeIndex(df['registered_at']).day
    return df

# # slicer = lambda datetime_text: datetime_text[11:13]


def slicer(datetime_text): return datetime_text[11:13]


def getHour(df):
    df['registered_hour'] = df['ConvertedDate'].apply(slicer)
    df['registered_hour'] = df['registered_hour'].astype(int)
    df = df[['id', 'name', 'email', 'registered_at', 'registered_year',
             'registered_month', 'registered_day', 'registered_hour']]
    df.to_csv('transformed_Datasets/customers.csv', index=False)


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
    tdf = tdf[['id', 'customer_id', 'item_id', 'date', 'bank_id', 'qty',
               'transaction_year', 'transaction_month', 'transaction_day', 'quarter']]
    tdf.to_csv('transformed_Datasets/transactions.csv', index=False)


transformTransactions()


def transform_others():
    banks_df = pd.read_csv('Datasets/banks.csv')
    banks_df = banks_df.rename(
        columns={'_id': 'id', 'code': 'bank_code', 'name': 'bank_name'})
    banks_df = banks_df[['id', 'bank_name', 'bank_code']]
    banks_df.to_csv('transformed_Datasets/banks.csv', index=False)

    ex_rate_df = pd.read_csv('Datasets/exchange_rates.csv')
    ex_rate_df = ex_rate_df.rename(
        columns={'rate': 'exchange_rate'})
    ex_rate_df = ex_rate_df[['exchange_rate', 'bank_id', 'date']]
    ex_rate_df.to_csv('transformed_Datasets/exchange_rates.csv', index=False)

    items_df = pd.read_csv('Datasets/items.csv')
    items_df = items_df.rename(
        columns={'name': 'truck_name', })
    items_df = items_df[['id', 'truck_name', 'cost_price', 'selling_price']]
    items_df.to_csv('transformed_Datasets/items.csv', index=False)


transform_others()
