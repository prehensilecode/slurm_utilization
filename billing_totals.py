#!/usr/bin/env python3
import sys
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import pandas as pd
import delorean
from delorean import Delorean
from datetime import datetime, timedelta
import calendar
import argparse
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


DEBUG_P = True


def read_charges(filenames):
    global DEBUG_P

    if DEBUG_P:
        for fn in filenames:
            print(f'DEBUG: read_charges(): fn = {fn}')
        print()

    charges_df = pd.concat((pd.read_csv(f, delimiter=',') for f in filenames), ignore_index=True)
    charges_df.reindex()

    if DEBUG_P:
        print(f'DEBUG: read_charges(): describe\n{charges_df.describe()}')
        print()
        print(f'DEBUG: read_charges(): head(10)\n{charges_df.head(10)}')
        print()

    return charges_df.copy()


def main():
    global DEBUG_P

    parser = argparse.ArgumentParser(description='Compute billing totals')
    parser.add_argument('-d', '--debug', action='store_true', help='Debugging output')
    parser.add_argument('-S', '--start', default=None, help='Month to start computing utilization in format YYYY-MM')
    parser.add_argument('-E', '--end', default=None, help='Month to end compute utilization (inclusive) in format YYYY-MM')
    parser.add_argument('-D', '--data-dir', required=True, help='Directory containing sacct output files')

    args = parser.parse_args()

    DEBUG_P = args.debug

    if DEBUG_P:
        print(f'DEBUG: args = {args}')

    # check data_dir
    if not os.path.isdir(args.data_dir):
        print(f'ERROR: given data_dir={args.data_dir} is not a directory')
        sys.exit(3)

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
    last_day_of_month = calendar.monthrange(year, month)[-1]
    one_day = timedelta(days=1)
    one_sec = timedelta(seconds=1)
    end_date = datetime(year=year, month=month, day=last_day_of_month) + one_day - one_sec

    if end_date < start_date:
        print(f'ERROR: end date {args.end} earlier than start date {args.start}')
        sys.exit(1)

    if DEBUG_P:
        print(f'DEBUG: start_date = {start_date}; type(start_date) = {type(start_date)}')
        print(f'DEBUG: end_date = {end_date}; type(end_date) = {type(end_date)}')

    # Data filenames are Data/Charges/picotte_charges_YYYYMM.csv

    # Generate list of dates
    dates = []
    if end_date > start_date:
        for stop in delorean.stops(freq=delorean.MONTHLY, start=start_date, stop=end_date):
            dates.append(stop.datetime)
    else:
        dates.append(start_date)
    date_strings = [f'{d.year}{d.month:02d}' for d in dates]

    if DEBUG_P:
        for d in dates:
            print(f'DEBUG: main(): date = {d}')
        print()

        for d in date_strings:
            print(f'DEBUG: main(): date_str = {d}')
        print()

    # Generate list of filenames
    filenames = []
    for dstr in date_strings:
        filenames.append(f'{args.data_dir}/picotte_charges_{dstr}.csv')

    if DEBUG_P:
        for f in filenames:
            print(f'DEBUG: main(): f = {f}')
        print()

    charges_df = read_charges(filenames)

    if DEBUG_P:
        print(f'DEBUG: main(): charges_df.describe() =\n{charges_df.describe()}')
        print()
        print(f'DEBUG: main(): charges_df.head(10) =\n{charges_df.head(10)}')
        print()

    sum_charges_df = charges_df[['Project', 'Total charge ($)']].groupby('Project').sum().sort_values(by=['Total charge ($)'], ascending=False).reset_index()

    sum_charges_df = sum_charges_df[sum_charges_df['Total charge ($)'] > 0.]

    if DEBUG_P:
        print(f'DEBUG: main(): sum_charges_df.describe() =\n{sum_charges_df.describe()}')
        print()
        print(f'DEBUG: main(): sum_charges_df.head(10) =\n{sum_charges_df.head(10)}')
        print()

    with open(f'total_charges_{date_strings[0]}-{date_strings[-1]}.csv', 'w') as f:
        sum_charges_df.to_csv(f)

    # seaborn plot
    # 3 bars: bigmem, gpu, standard
    sns.set_theme()  # set seqborn defaults
    sns.set_style('whitegrid')
    sns.color_palette('colorblind')
    sns.set(rc={'figure.figsize': (10., 7.5)})
    sns.set_context('paper', rc={'font.size': 8})

    paper_dims = (11., 8.5)
    fig, ax = plt.subplots(figsize=paper_dims)

    sns.barplot(ax=ax, data=sum_charges_df, x='Total charge ($)', y='Project', color='steelblue')

    plt.xlabel('Total charge ($)')
    plt.ylabel('Account')
    title_font = {'color': 'black', 'weight': 'bold', 'size': 14}
    plt.title(f'Picotte charges by account {start_date:%b %Y} to {end_date:%b %Y}', fontdict=title_font)
    plt.savefig(f'total_charges_plot_{date_strings[0]}-{date_strings[-1]}.png', dpi=300)
    plt.clf()


if __name__ == '__main__':
    main()

