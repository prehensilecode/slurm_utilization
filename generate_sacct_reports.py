#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse
from datetime import datetime, timedelta, date
import delorean
from delorean import Delorean
import calendar

DEBUG_P = True

# NB to see all jobs, this must be run as root
# Command template
#    sacct -P -r {partition_list} -S YYYY-MM-01 -E YYYY-{MM+1}-01 -o "JobID%20,JobName,User,Account%25,Partition,NodeList%20,Elapsed,State,ExitCode,ReqCPUS,ReqMem,MaxRSS,MaxVMSize,ReqTRES%60,AllocTRES%60"


def main():
    parser = argparse.ArgumentParser(description='Compute cluster utilization by partition from sacct output')
    parser.add_argument('-d', '--debug', action='store_true', help='Debugging output')
    parser.add_argument('-S', '--start', default=None, help='Month to start computing utilization in format YYYY-MM')
    parser.add_argument('-E', '--end', default=None, help='Month to end compute utilization (inclusive) in format YYYY-MM')
    args = parser.parse_args()

    # want "partitions" to be sets of distinct hosts
    partitions = ['def,long', 'gpu,gpulong', 'bm']

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
    dates = []
    for stop in delorean.stops(freq=delorean.MONTHLY, start=start_date, stop=end_date):
        dates.append(stop)

    ndates = len(dates)
    for d in range(ndates - 1):
        if DEBUG_P:
            print(dates[d])

        for p in partitions:
            filename = f'sacct_{p.split(",")[0]}_{dates[d].datetime.strftime("%Y%m")}.csv'
            start_date = dates[d].datetime.strftime('%Y-%m-%d')
            end_date = dates[d+1].datetime.strftime('%Y-%m-%d')
            sacct_cmd = f'sacct -P -r {p} -S {start_date} -E {end_date} -o JobID%20,JobName,User,Account%25,Partition,NodeList%20,Elapsed,State,ExitCode,ReqCPUS,ReqMem,MaxRSS,MaxVMSize,ReqTRES%60,AllocTRES%60'

            with open(filename, 'w') as outfile:
                subprocess.run(sacct_cmd.split(), stdout=outfile)


if __name__ == '__main__':
    main()

