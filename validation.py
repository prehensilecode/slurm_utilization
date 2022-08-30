#!/usr/bin/env python3
import sys
import os
import pandas as pd
import numpy as np


def main():
    # Pandas settings
    pd.set_option('mode.chained_assignment', 'raise')

    # Pandas display options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    gpu_validation_df = pd.read_csv('Data/PicotteAllTime/foo3.csv', delimiter='|')

    print('gpu_validation_df.info():')
    print(gpu_validation_df.info())
    print()
    print(gpu_validation_df.describe())
    print()

    brief_df = gpu_validation_df[['JobID', 'Account', 'Partition', 'Elapsed', 'ReqCPUS', 'ReqMem', 'ReqTRES', 'AllocTRES']].copy(deep=True)

    print(brief_df.head(20))
    print('…')
    print(brief_df.tail(20))
    print()

    brief_df['Elapsed'].replace(to_replace=r'\-', value=' days ', regex=True, inplace=True)
    brief_df.loc[:, 'Elapsed'] = pd.to_timedelta(brief_df['Elapsed'])

    print(brief_df.head(20))
    print('…')
    print(brief_df.tail(20))
    print()

    total_elapsed = brief_df['Elapsed'].sum()
    print(f'Total elapsed time = {total_elapsed}')
    total_elapsed_seconds = total_elapsed.days * 24 * 3600 + total_elapsed.seconds + total_elapsed.microseconds
    print(f'                   = {total_elapsed_seconds:.6e} secs.')

    print(f'One year = {365 * 24 * 3600:.6e} secs.')


if __name__ == '__main__':
    main()

