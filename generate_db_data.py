#!/usr/bin/python

import sqlite3
import random
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta


def make_interface_data():
    '''
    Generate interface data. TODO improve this docstring
    '''
    print('generating data ...')
    interface_data = []
    # 5 different ips
    for i in tqdm(range(1, 6)):
        ip = '1.' + str(i)
        # 5 interfaces for each ip
        for j in range(1, 6):
            interface = chr(ord('a') - 1 + j + 5 * (i - 1))
            # add 4 months of hourly data for each interface
            d = datetime(year=2016, month=3, day=1, hour=0)
            end_date = datetime(year=2016, month=6, day=30, hour=23)
            delta = timedelta(hours=1)
            while d <= end_date:
                if d.weekday() < 5 and (7 < d.hour < 16):
                    usage = abs(np.random.normal(loc=70, scale=20))
                    if usage > 100:
                        usage = 100
                else:
                    usage = random.randint(0, 30)
                date = d.strftime("%Y-%m-%d %H:%M:%S")
                interface_data.append([ip, interface, date, usage])
                d += delta
    return interface_data


def create_interface_table(cursor, table_name):
    '''
    Create new sql table to hold interface data.
    '''
    sql_to_drop_table = '''
    DROP TABLE IF EXISTS {0}
    '''.format(table_name)
    cursor.execute(sql_to_drop_table)
    sql_to_create_table = '''
    CREATE TABLE {0} (
        ip VARCHAR(5),
        interface VARCHAR(5),
        date DATETIME,
        usage REAL
    )
    '''.format(table_name)
    cursor.execute(sql_to_create_table)


def insert_interface_record(cursor, table_name, record):
    '''
    Add the row of data in <record> to the sql table <table_name>
    '''
    sql = '''
    INSERT INTO {0} (ip, interface, date, usage)
    VALUES {1}
    '''.format(table_name, tuple(record))
    cursor.execute(sql)


def main():
    interface_data = make_interface_data()
    interface_table = 'Interface'
    conn = sqlite3.connect('Network_Data.db')
    cursor = conn.cursor()
    create_interface_table(cursor, interface_table)
    print('inserting data into database ...')
    for row in tqdm(interface_data):
        insert_interface_record(cursor, interface_table, row)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
