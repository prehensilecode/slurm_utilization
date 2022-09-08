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
import subprocess

### Command (to be run by root) to produce the appropriate sacct output:
###    see script generate_sacct_reports.py

DEBUG_P = True

# conversion from Gibi
KIBI = 1./(1024. * 1024.)
MEBI = 1./1024.
GIBI = 1.
TEBI = 1024.


def sreport_utilization(start_date=None, end_date=None):
    global DEBUG_P

    start_date_str = f'{start_date.year}-{start_date.month:02d}-01'
    end_date_str = f'{end_date.year}-{end_date.month:02d}-01'
    sreport_cmd = f'sreport -P cluster utilization Start={start_date_str} End={end_date_str} Format="Allocated,Down,PlannedDown,Idle,Planned,Reported"'.split(' ')

    sreport = subprocess.run(sreport_cmd, check=True, capture_output=True, text=True).stdout.strip().split('\n')[4:]

    colnames = sreport[0].split('|')
    colvals = [int(i) for i in sreport[1].split('|')]

    sreport_dict = dict(zip(colnames, colvals))

    if DEBUG_P:
        print(f'DEBUG: sreport_utilization: colnames = {colnames}')
        print(f'DEBUG: sreport_utilization: colvals = {colvals}')
        print(f'DEBUG: sreport_utilization: sreport_dict = {sreport_dict}')

    # sreport displays CPU-minutes; want CPU-days
    minutes_per_day  = 24. * 60.

    utilization = sreport_dict["Allocated"]/sreport_dict["Reported"] * 100.
    reported_minutes = sreport_dict["Reported"] / (88. * 48.)

    if DEBUG_P:
        print(f'DEBUG: sreport_utilization: reported_minutes (to days) = {reported_minutes/(24.*60.)}')

    print()
    print(f'CLUSTER UTILIZATION from sreport ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d})')
    print(f'Reported:     {sreport_dict["Reported"] / minutes_per_day:.05e} CPU-days')
    print(f'Allocated:    {sreport_dict["Allocated"] / minutes_per_day:.05e} CPU-days ({sreport_dict["Allocated"]/sreport_dict["Reported"]*100.:5.02f}%)')
    print(f'Down:         {sreport_dict["Down"] / minutes_per_day:.05e} CPU-days ({sreport_dict["Down"]/sreport_dict["Reported"]*100.:5.02f}%)')
    print(f'Planned down: {sreport_dict["PLND Down"] / minutes_per_day:.05e} CPU-days')
    print(f'Idle:         {sreport_dict["Idle"] / minutes_per_day:.05e} CPU-days ({sreport_dict["Idle"]/sreport_dict["Reported"]*100.:5.02f}%)')
    print(f'Utilization:  {utilization:.02f} %')

    return utilization, reported_minutes


def utilization_gpu(gpu_sacct_df=None, start_date=None, end_date=None):
    global DEBUG_P

    tres_of_interest = 'gres/gpu'

    if DEBUG_P:
        print(f'DEBUG: utilization(): tres_of_interest = {tres_of_interest}')

    # drop rows without "gres/gpu=" since some jobs in gpu
    # partition did not request gres/gpu
    gpu_sacct_df = gpu_sacct_df[gpu_sacct_df['ReqTRES'].str.contains(f'{tres_of_interest}=', regex=False)].copy()

    if DEBUG_P:
        print('DEBUG utilization_gpu(): gpu_sacct_df.head(20)')
        print(gpu_sacct_df.head(20))
        print('')

        print('DEBUG utilization_gpu(): gpu_sacct_df.tail(20)')
        print(gpu_sacct_df.tail(20))
        print('')

        with open('gpu_sacct_df.csv', 'w') as f:
            gpu_sacct_df.to_csv(f, index=False)

    to_numeric(gpu_sacct_df['ReqGPUS'])

    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df["ReqGPUS"].head(20) = {gpu_sacct_df["ReqGPUS"].head(20)}')
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df["ReqGPUS"].tail(20) = {gpu_sacct_df["ReqGPUS"].tail(20)}')
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df["ReqGPUS"].describe() = {gpu_sacct_df["ReqGPUS"].describe()}')

    gpu_sacct_df['GPUseconds'] = gpu_sacct_df[['Elapsed', 'ReqGPUS']].product(axis=1)

    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df["GPUseconds"].head(20) = {gpu_sacct_df["GPUseconds"].head(20)}')
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df["GPUseconds"].tail(20) = {gpu_sacct_df["GPUseconds"].tail(20)}')
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df.describe() = \n{gpu_sacct_df.describe()}')
        print(f'DEBUG: utilization_gpu(): Total number of GPU jobs = {len(gpu_sacct_df.index)}')
        print(f'DEBUG: utilization_gpu(): Total seconds in a year = {3600 * 24 * 365:.4e}')
        print(f'DEBUG: utilization_gpu(): Total GPUseconds utilized = {gpu_sacct_df["GPUseconds"].sum():.4e}')

    # N.B. this does not take downtime into account
    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): start_date = {start_date}; end_date = {end_date}')

    period_of_interest = end_date - start_date

    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): period_of_interest days = {period_of_interest.days + period_of_interest.seconds / 86400.:.02f}')

    max_gpudays = 12. * 4. * (period_of_interest.days + period_of_interest.seconds / 86400.)

    total_gpudays_allocated = gpu_sacct_df['GPUseconds'].sum() / 86400.

    print()
    print(f'GPU UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d})')
    print(f'No. of GPU jobs: {len(gpu_sacct_df.index):,}')
    print(f'Total available: {max_gpudays:.5e} GPU-days')
    print(f'Allocated:       {total_gpudays_allocated:.5e} GPU-days')

    gpu_util = total_gpudays_allocated / max_gpudays * 100.
    print(f'GPU utilization: {gpu_util:.2f} %')

    return gpu_util


def convert_to_GiB(memstr):
    global DEBUG_P
    global KIBI
    global MEBI
    global GIBI
    global TEBI

    unit_list = [KIBI, MEBI, GIBI, TEBI]
    prefix_list = ['K', 'M', 'G', 'T']

    unit_dict = dict(zip(prefix_list, unit_list))

    unit = memstr[-1]
    amt = memstr[:-1]

    return float(amt) * unit_dict[unit]


def utilization_bm(bm_sacct_df=None, start_date=None, end_date=None):
    global DEBUG_P

    # drop rows with Na ReqMem field
    bm_sacct_df.dropna(subset='ReqMem')

    # need to convert ReqMem field to GiB; values read in are
    # strings with last character being K,M,G,T, etc (why isn't it "k" instead of "K"?)

    bm_sacct_df['ReqMem'] = bm_sacct_df['ReqMem'].apply(convert_to_GiB)

    # N.B. this does not take downtime into account
    period_of_interest = end_date - start_date

    if DEBUG_P:
        print(f'DEBUG utilization_bm(): period_of_interest = {period_of_interest}')

    # no. nodes * mem. per node (~1.5 TiB = 1546 GiB) * tot. seconds
    max_memseconds = 2. * 1546. * period_of_interest.total_seconds()

    if DEBUG_P:
        print(f'DEBUG utilization_bm(): max_memseconds = {max_memseconds}')

    # for bm partition, look at the ReqMem column
    # compute "mem-seconds"
    bm_sacct_df['MemSeconds'] = bm_sacct_df[['Elapsed', 'ReqMem']].product(axis=1)

    if DEBUG_P:
        print(f'DEBUG: utilization_bm: bm_sacct_df.describe() = {bm_sacct_df.describe()}')

        convert_to_GiB('123K')
        convert_to_GiB('456M')
        convert_to_GiB('789G')
        convert_to_GiB('321T')

    total_memseconds_allocated = bm_sacct_df['MemSeconds'].sum()

    bm_util = total_memseconds_allocated / max_memseconds

    print()
    print(f'BIGMEM UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d})')
    print(f'No. of bigmem jobs: {len(bm_sacct_df.index):,}')
    print(f'Total available:    {max_memseconds/86400.:.5e} GiB-days')
    print(f'Allocated:          {total_memseconds_allocated/86400.:.5e} GiB-days')
    print(f'Bigmem utilization: {bm_util:.2f} %')

    return bm_util


def utilization_def(def_sacct_df=None, start_date=None, end_date=None):
    global DEBUG_P

    # drop rows with Na ReqCPUS field
    def_sacct_df.dropna(subset='ReqCPUS')

    # N.B. this does not take downtime into account
    period_of_interest = end_date - start_date

    if DEBUG_P:
        print(f'DEBUG utilization_def(): period_of_interest = {period_of_interest}')

    # no. of nodes * no. of cores per node * tot. days
    max_cpudays = 74. * 48. * period_of_interest.total_seconds() / 86400.

    # create a CPUseconds column
    def_sacct_df['CPUseconds'] = def_sacct_df[['Elapsed', 'ReqCPUS']].product(axis=1)

    total_cpudays_allocated = def_sacct_df['CPUseconds'].sum() / 86400.

    cpu_util = 0.

    print()
    print(f'CPU ("def" partition) UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d})')
    print(f'No. of def jobs: {len(def_sacct_df.index):,}')
    print(f'Total available: {max_cpudays:.5e} CPU-days')
    print(f'Allocated:       {total_cpudays_allocated:.5e} CPU-days')

    cpu_util = total_cpudays_allocated / max_cpudays * 100.
    print(f'CPU utilization: {cpu_util:.2f} %')

    return cpu_util


def utilization_billing(billing_sacct_df):
    pass


def utilization(partition='def', sacct_df=None, start_date=None, end_date=None, use_billing=False):
    global DEBUG_P

    if DEBUG_P:
        print(f'DEBUG: utilization(): partition = {partition}')
        print(f'DEBUG: utilization(): sacct_df.head(20) = ')
        print(sacct_df.head(20))

    if DEBUG_P:
        print(f'DEBUG: utilization(): start_date = {start_date}; end_date = {end_date}')

    # XXX for 'bm' and 'def', look at the 'ReqMem' and 'ReqCPUS' column.
    # XXX for 'gpu', look at the ReqTRES column, and search for 'gres/gpu'

    tres_of_interest = None

    utilization = 0.

    if not use_billing:
        if partition == 'gpu':
            gpu_sacct_df = sacct_df[(sacct_df['Partition'] == 'gpu') | (sacct_df['Partition'] == 'gpulong')].copy(deep=True)
            utilization = utilization_gpu(gpu_sacct_df, start_date, end_date)
        elif partition == 'bm':
            bm_sacct_df = sacct_df[(sacct_df['Partition'] == 'bm')].copy(deep=True)
            utilization = utilization_bm(bm_sacct_df, start_date, end_date)
        elif partition == 'def':
            def_sacct_df = sacct_df[(sacct_df['Partition'] == 'def')].copy(deep=True)
            utilization = utilization_def(def_sacct_df, start_date, end_date)
    else:
        pass

    return utilization


def read_sacct(filenames):
    global DEBUG_P

    if DEBUG_P:
        print(f'DEBUG read_sacct(): filenames = {filenames}')
        print()

    sacct_df = pd.concat((pd.read_csv(f, delimiter='@') for f in filenames), ignore_index=True)

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

    # Data filenames are Data/PicotteAllTime/sacct_{partition}_YYYYMM.csv

    # Generate list of dates
    dates = []
    if end_date > start_date:
        for stop in delorean.stops(freq=delorean.MONTHLY, start=start_date, stop=end_date):
            dates.append(stop.datetime)
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
            print(f'DEBUG main() filenames - : {f}')

    sacct_df = read_sacct(filenames)

    if DEBUG_P:
        print('DEBUG main(): just read sacct_df')
        print(sacct_df.info())
        print('')

    # drop jobs with no assigned nodes
    sacct_df = sacct_df[sacct_df['NodeList'] != 'None assigned']

    if DEBUG_P:
        print('DEBUG utilization(): after dropping "NodeList == None assigned"')
        print(sacct_df.info())
        print('')

    # drop the "batch" and "extern" rows
    sacct_df = sacct_df[sacct_df['JobName'] != 'batch']
    sacct_df = sacct_df[sacct_df['JobName'] != 'extern']

    # keep rows where ReqTRES is not NaN
    sacct_df = sacct_df[sacct_df['ReqTRES'].notna()]

    # keep only jobs not by urcftestprj
    sacct_df = sacct_df[sacct_df['Account'] != 'urcftestprj']

    # format the Elapsed field
    sacct_df['Elapsed'].replace(to_replace=r'\-', value=' days ', regex=True, inplace=True)

    # convert Elapsed column to seconds
    sacct_df.loc[:, 'Elapsed'] = pd.to_timedelta(sacct_df['Elapsed'])
    sacct_df.loc[:, 'Elapsed'] = sacct_df['Elapsed'].dt.total_seconds()

    sacct_df = sacct_df[['JobID', 'Account', 'Partition', 'Elapsed', 'ReqCPUS', 'ReqMem', 'ReqTRES', 'AllocTRES']]
    sacct_df = sacct_df.dropna()

    if DEBUG_P:
        print('DEBUG: utilization(): INFO')
        print(sacct_df.info())

    util = {}
    util['general'], uptime_minutes = sreport_utilization(start_date, end_date)
    util['gpu'] = utilization('gpu', sacct_df=sacct_df, start_date=start_date, end_date=end_date, use_billing=use_billing)
    util['bm'] = utilization('bm', sacct_df=sacct_df, start_date=start_date, end_date=end_date, use_billing=use_billing)
    util['def'] = utilization('def', sacct_df=sacct_df, start_date=start_date, end_date=end_date, use_billing=use_billing)

    if DEBUG_P:
        print(f'DEBUG: util = {util}')


if __name__ == '__main__':
    main()

