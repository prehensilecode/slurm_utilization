#!/usr/bin/env python3
import sys
import os
import subprocess
from datetime import datetime
import delorean
from delorean import Delorean

DEBUG_P = True

# NB to see all jobs, this must be run as root
# Command template
#    sacct -P -r {partition_list} -S YYYY-MM-01 -E YYYY-{MM+1}-01 -o "JobID%20,JobName,User,Account%25,Partition,NodeList%20,Elapsed,State,ExitCode,ReqMem,MaxRSS,MaxVMSize,AllocTRES%60"


def main():
    # want "partitions" to be sets of distinct hosts
    partitions = ['def,long', 'gpu,gpulong', 'bm']

    date_start = datetime(year=2021, month=2, day=1)
    date_stop = datetime(year=2022, month=8, day=1)

    dates = []
    for stop in delorean.stops(freq=delorean.MONTHLY, start=date_start, stop=date_stop):
        dates.append(stop)

    ndates = len(dates)
    for d in range(ndates - 1):
        print(dates[d])
        for p in partitions:
            filename = f'sacct_{p.split(",")[0]}_{dates[d].datetime.strftime("%Y%m")}.csv'
            start_date = dates[d].datetime.strftime('%Y-%m-%d')
            end_date = dates[d+1].datetime.strftime('%Y-%m-%d')
            sacct_cmd = f'sacct -P -r {p} -S {start_date} -E {end_date} -o JobID%20,JobName,User,Account%25,Partition,NodeList%20,Elapsed,State,ExitCode,ReqMem,MaxRSS,MaxVMSize,AllocTRES%60'
            if DEBUG_P:
                print(f'DEBUG: sacct_cmd = {sacct_cmd}')
                print(f'DEBUG: sacct_cmd.split() = {sacct_cmd.split()}')
            with open(filename, 'w') as outfile:
                subprocess.run(sacct_cmd.split(), stdout=outfile)


if __name__ == '__main__':
    main()

