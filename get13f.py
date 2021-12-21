#!/usr/bin/python3
import requests
import pandas
import sqlite3 as sql
from os.path import exists
import datetime as dt
import lxml.html as lh

db_path = './all13f.db'

funds_dict = {
        '1079114': 'Green Light Capital (Einhorn)',
        '1040273': 'Third Point (Loeb)',
        '1103804': 'Viking Global Investors',
        '1061165': 'Lone Pine',
        '1389507': 'Discovery Capital',
        '1135730': 'Coatue Management',
        '934639': 'Maverick Capital',
        '1167483': 'Tiger Global Management',
        '1656456': 'Appaloosa Mangement (Tepper)'
        }
dateMin = dt.date(2021,9,1)
dateMax = dt.date(2022,1,1)

print('Using db', db_path)
print('Searching reports between ', dateMin, ' and ', dateMax)

headers = {'user-agent': 'my-app/0.1.0'}
xsl = '''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:n1="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <xsl:output method="xml" indent="yes" />
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="n1:infoTable">
    <xsl:copy>
     <xsl:apply-templates select="n1:nameOfIssuer" />
     <xsl:apply-templates select="n1:titleOfClass" />
     <xsl:apply-templates select="n1:cusip" />
     <xsl:apply-templates select="n1:value" />
     <xsl:apply-templates select="n1:*/n1:sshPrnamt" />
     <xsl:apply-templates select="n1:*/n1:sshPrnamtType" />
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>
'''

if not exists(db_path):
    conn = sql.connect(db_path)
    c = conn.cursor()
    c.executescript('''
    CREATE TABLE SEC13F8 ("index" PRIMARY KEY NOT NULL,
    "issuer" TEXT,
    "cusip" TEXT,
    "value" INT,
    "shares" INT,
    "cik" TEXT,
    "date" DATE
     );
     ''')
    c.executescript('''
    CREATE TABLE FUNDS ("cik" PRIMARY KEY NOT NULL,
    "name" TEXT
     );
     ''')
    c.close()
else:
    conn = sql.connect(db_path)

# Store fund names into table
query = 'INSERT OR IGNORE INTO FUNDS VALUES ' + ','.join([f" ('{item[0]}', '{item[1]}')" for item in funds_dict.items()])
c = conn.cursor()
c.executescript(query)
c.close()

for item in funds_dict.items():
    id = item[0]
    fund_name = item[1]
    print('Researching Fund:', fund_name)
    url = 'https://data.sec.gov/submissions/CIK' + id.zfill(10) + '.json'
    response = requests.get(url, headers=headers)
    json = response.json()
    filings = json['filings']['recent']
    for i in range(len(filings['accessionNumber'])):
        if '13F' in filings['form'][i]:
            reportDate = filings['reportDate'][i]
            reportDate_dt = dt.date.fromisoformat(reportDate)
            if reportDate_dt >= dateMin and reportDate_dt <= dateMax:
                accnum = filings['accessionNumber'][i]
                print('Getting 13F on', reportDate, ', an:', accnum)
                
                # Find url of infotable
                index_url = 'https://www.sec.gov/Archives/edgar/data/' + id.zfill(10) + '/' + accnum.replace('-','') + '/' + accnum + '-index.htm'
                response = requests.get(index_url, headers=headers)
                df_index = pandas.read_html(response.text)
                df_index = df_index[0]
                doc_select = df_index.Type.str.contains('INFORMATION') & df_index.Document.str.contains('.xml')
                assert(any(doc_select)), 'No INFORMATION and .xml entries found!'
                info_url = df_index.loc[doc_select, 'Document'].item().strip()
                
                # Get the info table
                data_url = 'https://www.sec.gov/Archives/edgar/data/' + id + '/' + accnum.replace('-','') + '/' + info_url
                response = requests.get(data_url, headers=headers)
                
                # Store xml as dataframe, clean up names
                df = pandas.read_xml(response.text, stylesheet=xsl)
                df.rename(columns={'nameOfIssuer': 'issuer', 'sshPrnamt': 'shares', 'sshPrnamtType': 'type'}, inplace=True)
                
                # Look for entrys that don't list shares
                if any(df['type'] != 'SH'):
                    print('Got some non-shares data:')
                    print(df[df['type'] != 'SH']['type'])
                df.loc[df['type'] != 'SH', 'shares'] = 0
                
                # Combine rows with same company and select relevant columns
                df_combined = df.groupby('cusip').agg({'issuer': 'first', 'cusip': 'first', 'value': 'sum', 'shares': 'sum'})
                
                # Store in SQL
                df_combined['date'] = reportDate
                df_combined['cik'] = id
                df_combined['index'] = reportDate.replace('-','') + id.zfill(10) + df_combined['cusip']
                print('Storing in db.')
                try:
                    df_combined.to_sql('SEC13F8',conn,if_exists='append',index=False)
                except sql.IntegrityError as err:
                    if 'UNIQUE' in str(err):
                        print('Entries already exist.')
                    else:
                        raise(err)