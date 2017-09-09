__author__ = "Ouvek Kostiva"
__copyright__ = "2017"
__credits__ = ["Huang Hsin Yuan","Ouvek Kostiva"]
__maintainer__ = "Huang Hsin Yuan"
__email__ = "kostiva@ouvek.com"
__status__ = "Prototype"

def getTWSEdata(stockCode="1301", monthDate="20170801"):
    # monthDate = 20170801
    import requests
    import pickle
    r = requests.get('http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={}&stockNo={}'.format(monthDate,stockCode))
    if not os.path.exists("Pickles"):
        os.makedirs("Pickles")
    output = open('Pickles/{}_{}.pkl'.format(stockCode,monthDate), 'wb')
    print("Downloaded:","{}_{}.pkl".format(stockCode,monthDate))
    pickle.dump(r.text, output)
    pklname = '{}_{}.pkl'.format(stockCode,monthDate)
    return pklname

def readPickledData(fileName):
    import os
    import pickle
    if os.path.isfile('Pickles/{}.pkl'.format(fileName)):
        pkl_file = open('Pickles/{}.pkl'.format(fileName), 'rb')
        print("Pickled File ", fileName, " Loaded")
        import json
        data = json.loads(pkl_file)
    else:
        print("Filename should be like: code_duration, ex:1103_20170801")
        
def createDatabase(dbName,tableName):
    import os
    if os.path.isfile(dbName):
        return "Database name already exists: ", dbName
    else:    
        import sqlite3
        sqlite_file = dbName
        conn = sqlite3.connect(sqlite_file)
        c = conn.cursor()
        c.execute('CREATE TABLE {tn} (pk INTEGER PRIMARY KEY, code TEXT NOT NULL, name TEXT, date TEXT NOT NULL, sharesvol REAL, dollarvol REAL, open REAL, high REAL, low REAL, close REAL, diffr REAL, trades REAL)'.format(tn=tableName)) 
        conn.commit()
        conn.close()
        return "Database",dbName," Successfully Created!"
    
def toDataList(fileName):
    datalist = []
    import datetime as dt
    file = readPickledData(fileName)
    import json
    data = json.loads(file)
    title = data['title'].split() 
    #"代號  名稱 日期       成交股數   成交金額     開盤價 最高價 最低價 收盤價 漲跌差 成交筆數"
    for i in range(0,(len(daily)-1)):
        datalist.append([title[1], title[2], data['data'][i][0], data['data'][i][1], data['data'][i][2], data['data'][i][3], data['data'][i][4], data['data'][i][5], data['data'][i][6], data['data'][i][7], data['data'][i][8]])
    return dataList

def insertData(dbName, tableName, dataList):
    import sqlite3
    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    count = 0
    for code, name, date, sharesvol, dollarvol, ope, high, low, close, diffr, trades in dataList:
        conn.execute("INSERT INTO twse (code, name, date, sharesvol, dollarvol, open, high, low, close, diffr, trades) VALUES (?,?,?,?,?,?,?,?,?,?,?)",(code, name, date, sharesvol, dollarvol, ope, high, low, close, diffr, trades))
        count = count + 1
    conn.commit()
    conn.close()
    return count

def getMonthDate(years=1):
    months = years*12
    import datetime
    now = datetime.datetime.now()
    first = now.replace(day=1)
    yr = first.strftime("%Y")
    mon = first.strftime("%m")
    mon = int(mon)
    yr = int(yr)
    monthDates = []
    for i in range(1,months+1):
        if yr > 0:
            if mon <= 9:
                if mon <= 1:
                    yr = yr - 1
                    mon = 12
                    string = "{}0{}01".format(yr,mon)
                    monthDates.append(string)
                else:
                    mon = mon - 1
                    string = "{}0{}01".format(yr,mon)
                    monthDates.append(string)
            else:
                mon = mon - 1
                string = "{}{}01".format(yr,mon)
                monthDates.append(string)
    print(monthDates)
