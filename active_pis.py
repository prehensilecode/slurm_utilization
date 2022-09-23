#!/usr/bin/env python3
import sys
import os
import numpy as np
import pandas as pd
import delorean
from delorean import Delorean
from datetime import datetime, timedelta, date
import calendar
import argparse
import matplotlib as mpl
import matplotlib.pyplot as plt


DEBUG_P = True

def read_statements(filenames):
    global DEBUG_P

    if DEBUG_P:
        for fn in filenames:
            print(f'DEBUG: read_statements(): filename = {fn}')

    statements_df = pd.concat((pd.read_csv(f, encoding='latin1') for f in filenames), ignore_index=True,)

    if DEBUG_P:
        print('DEBUG read_statements(): Head')
        print(statements_df.head(5))
        print('')

        print('DEBUG read_sacct(): Info')
        statements_df.info()
        print('')

        print('DEBUG read_sacct(): Description')
        print(statements_df.describe())
        print('')

    statements_df.loc[:, 'Project'] = pd.Series(statements_df['Project'], dtype=pd.StringDtype())

    return statements_df.copy()



def main():
    global DEBUG_P

    parser = argparse.ArgumentParser(description='Print list of active research groups')
    parser.add_argument('-d', '--debug', action='store_true', help='Debugging output')
    parser.add_argument('-S', '--start', required=True, default=None, help='Month to start computing utilization in format YYYY-MM')
    parser.add_argument('-E', '--end', required=True, default=None, help='Month to end compute utilization (inclusive) in format YYYY-MM')

    args = parser.parse_args()

    DEBUG_P = args.debug

    # Pandas settings
    pd.set_option('mode.chained_assignment', 'raise')

    # Pandas display options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    start_date_str = None
    if not args.start:
        today = Delorean()
        last_month = today - timedelta(days=(today.datetime.day + 1))
        start_date_str = last_month.date.strftime('%Y-%m')
    else:
        start_date_str = args.start

    year, month = [int(i) for i in start_date_str.split('-')]
    start_date = datetime(year=year, month=month, day=1)

    end_date_str = None
    if not args.end:
        end_date_str = start_date_str
    else:
        end_date_str = args.end

    year, month = [int(i) for i in end_date_str.split('-')]
    end_date = datetime(year=year, month=month, day=1)

    if end_date < start_date:
        print(f'ERROR: end date {args.end} earlier than start date {args.start}')
        sys.exit(1)

    if DEBUG_P:
        print(f'DEBUG: start_date = {start_date}; type(start_date) = {type(start_date)}')
        print(f'DEBUG: end_date = {end_date}; type(end_date) = {type(end_date)}')

    # Data filenames are /ifs/sysadmin/RCM/YYYY-MM/picotte_charges_YYYYMM.csv

    # Generate list of dates
    dates = []
    if end_date > start_date:
        for stop in delorean.stops(freq=delorean.MONTHLY, start=start_date, stop=end_date):
            dates.append(stop.datetime)
    else:
        dates.append(start_date)

    if DEBUG_P:
        print(f'DEBUG: dates = {dates}')
        for d in dates:
            print(f'DEBUG: type(d) = {type(d)}')

    # generate list of filenames
    filenames = []
    for d in dates:
        filenames.append(f'/ifs/sysadmin/RCM/{d.year}-{d.month:02}/picotte_charges_{d.year}{d.month:02}.csv')

    if DEBUG_P:
        print(f'DEBUG: filenames = {filenames}')

    statements_df = read_statements(filenames)

    if DEBUG_P:
        print(f'DEBUG: statements_df.describe() = {statements_df.describe()}')

    # drop uninteresting columns
    statements_df = statements_df.drop(['Cluster', 'Share expiration', 'Fund-Org code', 'Monthly credit?'], axis=1)

    # active projects are those where "Total charge ($)" is > 10.
    # write CSV list of active PIs
    statements_df[['Last name', 'First name', 'Email', 'Is MRI?']].loc[statements_df['Total charge ($)'] > 10.].drop_duplicates().to_csv('active_pis.csv', index=False)

    # list of PIs by total charge
    total_billed_df = statements_df[['Last name', 'First name', 'Email', 'Is MRI?', 'Total charge ($)']].loc[statements_df['Total charge ($)'] > 10.].groupby(['Last name', 'First name', 'Is MRI?'])['Total charge ($)'].sum().reset_index()

    total_billed_df.to_csv('foobar.csv', index=False)

    print(total_billed_df.sort_values(by='Total charge ($)', ascending=False).to_string(index=False))

    mpl.use('svg')

    total_billed_df.sort_values(by='Total charge ($)', ascending=False).plot.bar(x='Last name', y='Total charge ($)', rot=270, fontsize=7)
    plt.savefig('pi_charges.svg')



if __name__ == '__main__':
    main()
