#!/usr/bin/env python3
import sys
import os
import re
import pandas as pd
import numpy as np
import datetime

### Command (to be run by root) to produce the appropriate sacct output:
###    sacct -P -r gpu,gpulong -S 2021-02-01 -E 2022-08-01 -o "JobID%20,JobName,User,Account%25,NodeList%20,Elapsed,State,ExitCode,AllocTRES%60" > sacct.csv 2>&1

DEBUG_P = True


def read_sacct(filename):
    sacct_df = pd.read_csv(filename, delimiter='|')

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

    sacct_df['Account'] = pd.Series(sacct_df['Account'], dtype=pd.StringDtype())
    sacct_df['AllocTRES'] = pd.Series(sacct_df['AllocTRES'], dtype=pd.StringDtype())
    sacct_df['NodeList'] = pd.Series(sacct_df['NodeList'], dtype=pd.StringDtype())

    return sacct_df


def main():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    sacct_df = read_sacct('Data/sacct.csv')

    if DEBUG_P:
        print('DEBUG main(): just read sacct_df')
        print(sacct_df.info())
        print('')

    if DEBUG_P:
        print('DEBUG main(): after dropping mem columns')
        print(sacct_df.info())
        print('')

    # drop jobs with no assigned nodes
    sacct_df = sacct_df[sacct_df['NodeList'] != 'None assigned']

    # drop the "batch" and "extern" rows
    sacct_df = sacct_df[sacct_df['JobName'] != 'batch']
    sacct_df = sacct_df[sacct_df['JobName'] != 'extern']

    # drop rows without "gres/gpu="
    sacct_df = sacct_df[sacct_df['AllocTRES'].str.contains(r'gres/gpu=\d+')]

    if DEBUG_P:
        print('DEBUG main(): after dropping "NodeList == None assigned"')
        print(sacct_df.info())
        print('')

        print('DEBUG main(): all of sacct_df')
        print(sacct_df)
        print('')

    # drop jobs by urcftestprj
    sacct_df = sacct_df[sacct_df['Account'] != 'urcftestprj']

    if DEBUG_P:
        print('DEBUG main(): Info')
        print(sacct_df.info())
        print('')

        print('DEBUG main(): Description')
        print(sacct_df.describe())
        print('')

    sacct_df['Elapsed'] = sacct_df['Elapsed'].str.replace(r'\-', ' days ', regex=True)

    # convert Elapsed column to timedelta
    sacct_df['Elapsed'] = pd.to_timedelta(sacct_df['Elapsed'])
    sacct_df['Elapsed'] = sacct_df['Elapsed'].dt.total_seconds()

    sacct_df = sacct_df[['JobID', 'Account', 'Elapsed', 'AllocTRES']]
    sacct_df = sacct_df.dropna()

    print('INFO')
    print(sacct_df.info())
    print('')

    print('DESCRIBE')
    print(sacct_df.describe())
    print('')

    print('HEAD')
    print(sacct_df.head(20))
    print('')

    # want new column GPU-hours
    sacct_df['GPUcount'] = sacct_df['AllocTRES'].str.extract(r'gres/gpu=(\d+)')
    sacct_df['GPUcount'] = pd.to_numeric(sacct_df['GPUcount'])

    sacct_df['GPUseconds'] = sacct_df[['Elapsed', 'GPUcount']].product(axis=1)

    print('FOOBAR')
    print(sacct_df.info())

    print(f'Total GPUseconds = {sacct_df["GPUseconds"].sum()}')
    print(f'Total GPUhours = {sacct_df["GPUseconds"].sum() / 3600.}')

    #print('ALL OF sacct_df')
    #print(sacct_df)

    # Period of interest 2021-02-01 -- 2022-08-01
    start_time = datetime.datetime(2021, 2, 1)
    end_time = datetime.datetime(2022, 8, 1)

    dt = end_time - start_time
    print(f'dt = {dt}')

    max_gpuseconds = 12. * 4. * dt.total_seconds()

    print(f'Utilization = {sacct_df["GPUseconds"].sum() / max_gpuseconds * 100.}')

if __name__ == '__main__':
    main()

