import mysql.connector
import pandas as pd
import math

def flower_csv_tomysql(csv_path, tablename):
    #config
    cfg = {
        "host": "104.199.189.30",
        "port": 3306,
        "database": "camp_1",
        "user": "root",
        "password": ""
    }
    conn = mysql.connector.connect(**cfg)
    df = pd.read_csv(csv_path, encoding="utf-8", dtype='str')
    cursor = conn.cursor()
    print(df, math.ceil(df.count()[0] / 10000))
    for j in range(math.ceil(df.count()[0] / 10000)):
        end_count = (j + 1) * 10000
        if end_count > df.count()[0]:
            end_count = df.count()[0] % 10000 + 10000 * j
        sql_string = "insert into " + tablename + " values "
        for i in range(j * 10000, end_count):
            if i != j * 10000:
                sql_string += ","
            sql_string += "('" + "','".join(df.iloc[i]) + "')"
        print(sql_string)
        cursor.execute(sql_string)
        conn.commit()
    cursor.close()
    conn.close()


# csv路徑
csv_path = "./camp_url.csv"
# 要寫入的table名稱
tablename = "fff"
flower_csv_tomysql(csv_path,tablename)