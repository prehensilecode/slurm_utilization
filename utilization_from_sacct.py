#!/usr/bin/env python3
#    utilization_from_sacct.py - Compute utilization by reading sreport usage and normalizing against total available SUs.
#    Copyright (C) 2021  David Chin
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
import sys
import os
import re
import pandas as pd
import numpy as np
import delorean
from delorean import Delorean
from datetime import datetime, timedelta, date
import calendar
import argparse

### Command (to be run by root) to produce the appropriate sacct output:
###    see script generate_sacct_reports.py

DEBUG_P = True


def utilization(partition, sacct_df, use_billing=False):
    global DEBUG_P

    if DEBUG_P:
        print(f'DEBUG: utilization(): partition = {partition}')
        print(f'DEBUG: utilization(): sacct_df.head(20) = ')
        print(sacct_df.head(20))

    # Period of interest 2021-02-01 -- 2022-08-01
    start_time = datetime(2021, 2, 1)
    end_time = datetime(2022, 8, 1)

    period_of_interest = end_time - start_time

    if DEBUG_P:
        print(f'DEBUG: utilization(): period_of_interest = {period_of_interest}')

    # convert Elapsed column to timedelta
    sacct_df.loc[:, 'Elapsed'] = pd.to_timedelta(sacct_df['Elapsed'])
    sacct_df.loc[:, 'Elapsed'] = sacct_df['Elapsed'].dt.total_seconds()

    sacct_df = sacct_df[['JobID', 'Account', 'Partition', 'Elapsed', 'ReqCPUS', 'ReqMem', 'ReqTRES', 'AllocTRES']]
    sacct_df = sacct_df.dropna()

    if DEBUG_P:
        print('DEBUG: utilization(): INFO')
        print(sacct_df.info())

    # Dict of relevant TRES

    # XXX for 'bm' and 'def', look at the 'ReqMem' and 'ReqCPUS' column.
    # XXX for 'gpu', look at the ReqTRES column, and search for 'gres/gpu'

    tres_of_interest = None
    if partition == 'gpu':
        gpu_sacct_df = sacct_df[(sacct_df['Partition'] == 'gpu') | (sacct_df['Partition'] == 'gpulong')]
        if use_billing:
            tres_of_interest = 'billing'
        elif partition == 'gpu':
            tres_of_interest = 'gres/gpu'

        if DEBUG_P:
            print(f'DEBUG: utilization(): tres_of_interest = {tres_of_interest}')

        # drop rows without "gres/gpu=" since some jobs in gpu
        # partition did not request gres/gpu
        gpu_sacct_df = gpu_sacct_df[gpu_sacct_df['ReqTRES'].str.contains(f'{tres_of_interest}=', regex=False)].copy()

        if DEBUG_P:
            print('DEBUG utilization(): after dropping "NodeList == None assigned"')
            print(gpu_sacct_df.info())
            print('')

            print('DEBUG utilization(): all of sacct_df')
            print(gpu_sacct_df)
            print('')

            print('DEBUG utilization(): Info')
            print(gpu_sacct_df.info())
            print('')

            print('DEBUG utilization(): Description')
            print(gpu_sacct_df.describe())
            print('')


        print('')

        print('DESCRIBE')
        print(gpu_sacct_df.describe())
        print('')

        print('HEAD')
        print(gpu_sacct_df.head(20))
        print('')

        # want new column GPU-hours
        gpu_sacct_df['GPUcount'] = gpu_sacct_df['AllocTRES'].str.extract(r'gres/gpu=(\d+)')
        gpu_sacct_df['GPUcount'] = pd.to_numeric(gpu_sacct_df['GPUcount'])
        gpu_sacct_df['GPUseconds'] = gpu_sacct_df[['Elapsed', 'GPUcount']].product(axis=1)

        print('FOOBAR')
        print(gpu_sacct_df.info())

        print(f'Total GPUseconds = {gpu_sacct_df["GPUseconds"].sum()}')
        print(f'Total GPUhours = {gpu_sacct_df["GPUseconds"].sum() / 3600.}')

        # N.B. this does not take downtime into account
        max_gpuseconds = 12. * 4. * period_of_interest.total_seconds()

        gpu_util = gpu_sacct_df["GPUseconds"].sum() / max_gpuseconds * 100.
        print(f'GPU utilization = {gpu_sacct_df["GPUseconds"].sum() / max_gpuseconds * 100.}')

        return gpu_util



def read_sacct(filenames):
    global DEBUG_P

    sacct_df = pd.concat((pd.read_csv(f, delimiter='|') for f in filenames), ignore_index=True)

    if DEBUG_P:
        print('DEBUG read_sacct(): Head')
        print(sacct_df.head(5))
        print('')

        print('DEBUG read_sacct(): Info')
        sacct_df.info()
        print('')

        print('DEBUG read_sacct(): Description')
        print(sacct_df.describe())
        print('')

    sacct_df.loc[:, 'Account'] = pd.Series(sacct_df['Account'], dtype=pd.StringDtype())
    sacct_df.loc[:, 'ReqTRES'] = pd.Series(sacct_df['ReqTRES'], dtype=pd.StringDtype())
    sacct_df.loc[:, 'AllocTRES'] = pd.Series(sacct_df['AllocTRES'], dtype=pd.StringDtype())
    sacct_df.loc[:, 'NodeList'] = pd.Series(sacct_df['NodeList'], dtype=pd.StringDtype())

    return sacct_df.copy()


def main():
    global DEBUG_P

    parser = argparse.ArgumentParser(description='Compute cluster utilization by partition from sacct output')
    parser.add_argument('-d', '--debug', action='store_true', help='Debugging output')
    parser.add_argument('-S', '--start', default=None, help='Month to start computing utilization in format YYYY-MM')
    parser.add_argument('-E', '--end', default=None, help='Month to end compute utilization (inclusive) in format YYYY-MM')
    parser.add_argument('-b', '--billing', action='store_true', help='Use "billing" TRES for utilization')

    args = parser.parse_args()

    DEBUG_P = args.debug

    use_billing = args.billing

    if DEBUG_P:
        print(f'DEBUG: args = {args}')

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

    # Data filenames are Data/PicotteAllTime/sacct_{partition}_YYYYMM.csv

    # Generate list of dates
    dates = []
    if end_date > start_date:
        for stop in delorean.stops(freq=delorean.MONTHLY, start=start_date, stop=end_date):
            dates.append(stop)
    else:
        dates.append(start_date)
    date_strings = [f'{d.year}{d.month:02d}' for d in dates]

    if DEBUG_P:
        print(f'DEBUG: dates = {dates}')

    # List of partitions
    partitions = ['def', 'gpu', 'bm']

    # Generate list of filenames
    filenames = []
    for p in partitions:
        for dstr in date_strings:
            filenames.append(f'Data/PicotteAllTime/sacct_{p}_{dstr}.csv')

    if DEBUG_P:
        for f in filenames:
            print(f'DEBUG: {f}')

    sacct_df = read_sacct(filenames)

    if DEBUG_P:
        print('DEBUG main(): just read sacct_df')
        print(sacct_df.info())
        print('')

        print('DEBUG main(): after dropping mem columns')
        print(sacct_df.info())
        print('')

    # drop jobs with no assigned nodes
    sacct_df = sacct_df[sacct_df['NodeList'] != 'None assigned']

    # drop the "batch" and "extern" rows
    sacct_df = sacct_df[sacct_df['JobName'] != 'batch']
    sacct_df = sacct_df[sacct_df['JobName'] != 'extern']

    # drop the rows where ReqTRES is NaN
    mask = sacct_df['ReqTRES'].notna()
    sacct_df = sacct_df[mask]

    if DEBUG_P:
        print('DEBUG: main(): rows where job ID has a ".\d"')
        mask = sacct_df['JobID'].str.contains(r'\d+\.\d+')
        print(sacct_df[mask])


    # drop jobs by urcftestprj
    sacct_df = sacct_df[sacct_df['Account'] != 'urcftestprj']

    # format the Elapsed field
    sacct_df['Elapsed'].replace(to_replace=r'\-', value=' days ', regex=True, inplace=True)

    # convert Elapsed column to timedelta
    sacct_df.loc[:, 'Elapsed'] = pd.to_timedelta(sacct_df['Elapsed'])

    if DEBUG_P:
        print(f'DEBUG: main(): sacct_df.head(20) = {sacct_df.head(20)}')
    util = {}
    util['gpu'] = utilization('gpu', sacct_df, use_billing=use_billing)



if __name__ == '__main__':
    main()

