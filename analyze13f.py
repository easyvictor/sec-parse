#!/usr/bin/python3
from os.path import exists
import sqlite3 as sql
import pandas

db_path = './all13f.db'

if exists(db_path):
    conn = sql.connect(db_path)
else:
    print(db_path + ' does not exist. Exiting.')
    exit(1)
    
# Get funds
funds_df = pandas.read_sql_query('''SELECT * FROM "FUNDS"''', conn, index_col='cik')
funds_dict = funds_df.to_dict(orient='dict')['name']

top_funds = pandas.DataFrame()
cik_df = pandas.read_sql_query('''SELECT DISTINCT cik FROM "SEC13F8"''', conn)
for cik in cik_df['cik'].tolist():
    assert(cik), 'Not a valid cik lookup.'
    print('-----------------------')
    print('Analyzing Fund: ', funds_dict[cik])
    dates = pandas.read_sql_query('SELECT DISTINCT date from "SEC13F8" WHERE cik="' + cik + '" ORDER BY date ASC', conn, parse_dates='date')['date']
    dateMin = min(dates).date()
    dateMax = max(dates).date()
    if dateMin == dateMax:
        print('Reported:', dateMin)
    else:
        print('Reports between:', dateMin, 'and', dateMax)
    fund_df = pandas.read_sql_query('SELECT cusip, issuer, SUM(value) FROM "SEC13F8" WHERE cik="' + cik + '" GROUP BY cusip ORDER BY SUM(value) DESC', conn)
    fund_sum = fund_df['SUM(value)'].sum()
    print('Holdings: $%0.2fB' % (fund_sum/1e6))
    fund_pct = fund_df['SUM(value)']/fund_sum
    fund_df['pct'] = fund_pct
    top_df = fund_df[fund_pct > 0.02].copy()
    top_funds = pandas.concat([top_funds, top_df]).groupby('cusip', as_index=False).agg({'issuer': 'first', 'SUM(value)': 'sum','pct': 'sum'})
    print('Top stocks for fund:')
    top_df['SUM(value)'] = top_df['SUM(value)']/1000
    print(top_df.rename(columns={'issuer': 'Stock Issuer', 'SUM(value)': 'Value ($M)', 'pct': 'Sum Percentage'}))

top_funds.sort_values('pct', ascending=False, inplace=True)
top_funds.rename(columns={'SUM(value)': 'Value ($k)', 'pct': '% Fund Integrated'}, inplace=True)
print('--------------------------\n---------------------------')
print('Overall top funds, with percentage of portfolio integrated:')
print(top_funds.head(20))

all_df = pandas.read_sql_query('''SELECT cusip, issuer, cik, SUM(value), SUM(shares) FROM "SEC13F8" GROUP BY cusip ORDER BY SUM(value) DESC''', conn)


sum = all_df['SUM(value)'].sum()
pct = all_df['SUM(value)']/sum
all_df['pct'] = pct

top = all_df[pct > 0.02]
print('----------------------------')
print(top[['cusip', 'issuer', 'pct']].rename(columns={'issuer': 'Stock Issuer', 'pct': '% Total Value'}))
print('Funds: ', all_df.cik.nunique())
print('Total holdings: $%0.2fB' % (sum/1e6))
print('Number of investments >2% holding: ',len(top))