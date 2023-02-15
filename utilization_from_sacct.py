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
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
from enum import Enum
import pandas as pd
import delorean
from delorean import Delorean
from datetime import datetime, timedelta
import calendar
import argparse
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

### Command (to be run by root) to produce the appropriate sacct output:
###    see script generate_sacct_reports.py

DEBUG_P = True

# conversion from Gibi
KIBI = 1./(1024. * 1024.)
MEBI = 1./1024.
GIBI = 1.
TEBI = 1024.

# seconds per day
SECS_PER_DAY = 86400.

# seconds per hour
SECS_PER_HOUR = 3600.

# minutes per hour
MINS_PER_HOUR = 60.

# seconds per minute
SECS_PER_MINUTE = 60.

# hours per day
HOURS_PER_DAY = 24.

# minutes per day
MINS_PER_DAY = MINS_PER_HOUR * HOURS_PER_DAY

#
# Resources per node; keys are partition names
#
CPUS_PER_NODE = {'def': 48., 'gpu': 48., 'bm': 48.}

# N.B. mem is not installed RAM but total available for user jobs, reported by free(1)
# Units GiB
MEM_PER_NODE = {'def': 189., 'gpu': 189., 'bm': 1510.}

# GPUs per node
GPUS_PER_NODE = 4.

# billing per node
BILLING_PER_NODE = {'def': 48., 'gpu': 43. * 4., 'bm': 68. * 1.51}

# Nodes per partition
NODES_PER_PARTITION = {'def': 74., 'gpu': 12., 'bm': 2.}

# Total no. of nodes
TOTAL_NODES = NODES_PER_PARTITION['def'] + NODES_PER_PARTITION['gpu'] + NODES_PER_PARTITION['bm']

# Utilization by fraction of node
# - want to capture fraction of node utilized; e.g. a job in def partition
#   using only 1 CPU but 50% of memory should be 50% rather than 1/48 = 2%
#   and similar with GPUs
# - for memory, use the 'ReqMem' field
# - has to be done on a per-job basis


class UtilMethod(Enum):
    BY_RESOURCE = 'resource'
    BY_NODE = 'node'
    BY_BILLING = 'billing'


def nodedays(partition, period):
    # partition: def, bm, gpu
    # period: seconds
    return NODES_PER_PARTITION[partition] * period / SECS_PER_DAY


def convert_to_GiB(memstr):
    global DEBUG_P
    global KIBI
    global MEBI
    global GIBI
    global TEBI

    # have "?" because some jobs have misformatted ReqMem fields
    # which have "?" where a prefix is supposed to be
    unit_list = [KIBI, MEBI, GIBI, TEBI, GIBI]
    prefix_list = ['K', 'M', 'G', 'T', '?']

    unit_dict = dict(zip(prefix_list, unit_list))

    unit = memstr[-1]
    amt = memstr[:-1]

    return float(amt) * unit_dict[unit]


def sreport_utilization(start_date=None, end_date=None):
    global DEBUG_P

    orig_end_date = end_date
    orig_end_date_str = f'{orig_end_date.year}-{orig_end_date.month:02d}-01'

    if DEBUG_P:
        print(f'DEBUG: sreport_utilization(): start_date = {start_date.year}-{start_date.month:02d}')
        print(f'DEBUG: sreport_utilization(): end_date = {end_date.year}-{end_date.month:02d}')
        print(f'DEBUG: sreport_utilization(): orig_end_date = {orig_end_date.year}-{orig_end_date.month:02d}')

    # sreport start/end period is exclusive, so must add one month
    if end_date.month == 12:
        ed_year = end_date.year + 1
        ed_month = 1
    else:
        ed_year = end_date.year
        ed_month = end_date.month + 1

    end_date = datetime(year=ed_year, month=ed_month, day=1)

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

    utilization = sreport_dict["Allocated"]/sreport_dict["Reported"] * 100.
    # all nodes have 48 cpu cores
    reported_secs = sreport_dict["Reported"] * SECS_PER_MINUTE / (TOTAL_NODES * CPUS_PER_NODE['def'])

    if DEBUG_P:
        print(f'DEBUG: sreport_utilization: reported_secs (to days) = {reported_secs/SECS_PER_DAY}')

    cpu_mins_per_day = MINS_PER_DAY * TOTAL_NODES * CPUS_PER_NODE["def"]

    print()
    print(f'CLUSTER UTILIZATION from sreport based only on CPU usage ({start_date.year}-{start_date.month:02d} -- {orig_end_date.year}-{orig_end_date.month:02d} inclusive)')
    print(f'Reported:     {sreport_dict["Reported"] / MINS_PER_DAY:.05e} CPU-days - equiv. to {sreport_dict["Reported"] / cpu_mins_per_day:5.2f} days')
    print(f'Allocated:    {sreport_dict["Allocated"] / MINS_PER_DAY:.05e} CPU-days ({sreport_dict["Allocated"]/sreport_dict["Reported"]*100.:5.02f}%) - equiv. to {sreport_dict["Allocated"] / cpu_mins_per_day:5.2f} days')
    print(f'Down:         {sreport_dict["Down"] / MINS_PER_DAY:.05e} CPU-days ({sreport_dict["Down"]/sreport_dict["Reported"]*100.:5.02f}%) - equiv. to {sreport_dict["Down"] / cpu_mins_per_day:5.2f} days')
    print(f'Planned down: {sreport_dict["PLND Down"] / MINS_PER_DAY:.05e} CPU-days ({sreport_dict["PLND Down"]/sreport_dict["Reported"]*100.:5.02f}%) - equiv. to {sreport_dict["PLND Down"] / cpu_mins_per_day:5.2f} days')
    print(f'Idle:         {sreport_dict["Idle"] / MINS_PER_DAY:.05e} CPU-days ({sreport_dict["Idle"]/sreport_dict["Reported"]*100.:5.02f}%) - equiv. to {sreport_dict["Idle"] / cpu_mins_per_day:5.2f} days')
    print(f'Utilization:  {utilization:.02f} %')

    return utilization, reported_secs


def utilization_gpu(gpu_sacct_df=None, uptime_secs=None, start_date=None, end_date=None, util_method=None):
    global DEBUG_P
    global SECS_PER_DAY

    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df.nunique() before filtering =\n{gpu_sacct_df.nunique()}')

    tres_of_interest = 'gres/gpu'

    if DEBUG_P:
        print(f'DEBUG: utilization(): tres_of_interest = {tres_of_interest}')

    # pick only rows with "gres/gpu=" since some jobs in gpu
    # partition did not request gres/gpu
    gpu_sacct_df = gpu_sacct_df[gpu_sacct_df['ReqTRES'].str.contains(f'{tres_of_interest}=', regex=False)].copy()


    if DEBUG_P:
        print(f'DEBUG: utilization_gpu(): gpu_sacct_df.nunique() after filtering =\n{gpu_sacct_df.nunique()}')

        print('DEBUG utilization_gpu(): gpu_sacct_df.head(20)')
        print(gpu_sacct_df.head(20))
        print('')

        print('DEBUG utilization_gpu(): gpu_sacct_df.tail(20)')
        print(gpu_sacct_df.tail(20))
        print('')

        with open('gpu_sacct_df.csv', 'w') as f:
            gpu_sacct_df.to_csv(f, index=False)

    # want new column ReqGPUS (i.e. no. of GPUs requested)
    gpu_sacct_df['ReqGPUS'] = gpu_sacct_df['AllocTRES'].str.extract(r'gres/gpu=(\d+)')
    gpu_sacct_df['ReqGPUS'] = pd.to_numeric(gpu_sacct_df['ReqGPUS'])

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
        print(f'DEBUG: utilization_gpu(): uptime_secs = {uptime_secs}')

    if not uptime_secs:
        period_of_interest = end_date - start_date

        if DEBUG_P:
            print(f'DEBUG: utilization_gpu(): period_of_interest days = {period_of_interest.days + period_of_interest.seconds / 86400.:.02f}')

        max_nodedays = nodedays('gpu', period_of_interest.total_seconds())
    else:
        max_nodedays = nodedays('gpu', uptime_secs)

    if util_method == UtilMethod.BY_RESOURCE:
        max_gpudays = GPUS_PER_NODE * max_nodedays

        total_gpudays_allocated = gpu_sacct_df['GPUseconds'].sum() / SECS_PER_DAY

        print()
        print(f'GPU UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(gpu_sacct_df.index):,}')
        print(f'Total avail.: {max_gpudays:.5e} GPU-days')
        print(f'Allocated:    {total_gpudays_allocated:.5e} GPU-days')

        gpu_util = total_gpudays_allocated / max_gpudays * 100.
        print(f'GPU utilization: {gpu_util:.2f} %')

        # summary stats - no. of GPUs per job
        print()
        print(f'Mean no. of GPUs per job: {gpu_sacct_df["ReqGPUS"].mean():5.2f} (std. dev. {gpu_sacct_df["ReqGPUS"].std():4.2f})')
        print(f'Max. no. of GPUs per job: {gpu_sacct_df["ReqGPUS"].max()}')
    elif util_method == UtilMethod.BY_NODE:
        # take into account both CPUs and mem
        node_util = 0.

        # fix units of ReqMem
        gpu_sacct_df['ReqMem'] = gpu_sacct_df['ReqMem'].apply(convert_to_GiB)

        # create new column for by-node cost: max(fraction of cores, fraction of memory, fraction of GPU)
        gpu_sacct_df['FracNode'] = gpu_sacct_df.apply(lambda x: max(x.ReqCPUS / CPUS_PER_NODE['gpu'], x.ReqMem / MEM_PER_NODE['gpu'], x.ReqGPUS/GPUS_PER_NODE), axis=1)

        # create new column for fractional node * time
        gpu_sacct_df['FracNodeSeconds'] = gpu_sacct_df[['Elapsed', 'FracNode']].product(axis='columns')

        total_nodedays_allocated = gpu_sacct_df['FracNodeSeconds'].sum() / SECS_PER_DAY

        node_util = total_nodedays_allocated / max_nodedays * 100.

        print()
        print(f'GPU NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(gpu_sacct_df.index):,}')
        print(f'Total avail.: {max_nodedays:.5e} node-days')
        print(f'Allocated:    {total_nodedays_allocated:.5e} node-days')

        node_util = total_nodedays_allocated / max_nodedays * 100.
        print(f'Node utilization: {node_util:.2f} %')

        # summary stats - no. of GPUs per job
        print()
        print(f'Mean no. of GPUs per job: {gpu_sacct_df["ReqGPUS"].mean():5.2f} (std. dev. {gpu_sacct_df["ReqGPUS"].std():4.2f})')
        print(f'Max. no. of GPUs per job: {gpu_sacct_df["ReqGPUS"].max()}')

        gpu_util = node_util
    elif util_method == UtilMethod.BY_BILLING:
        max_su = BILLING_PER_NODE['gpu'] * max_nodedays
        total_su_allocated = gpu_sacct_df['SU'].sum() / HOURS_PER_DAY

        gpu_util = 0.

        print()
        print(f'GPU NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(gpu_sacct_df.index):,}')
        print(f'Total avail.: {max_su:.5e} SU')
        print(f'Allocated:    {total_su_allocated:.5e} SU')

        su_util = total_su_allocated / max_su * 100.
        print(f'SU utilization: {su_util:.2f} %')

        gpu_util = su_util

    return gpu_util, gpu_sacct_df


def utilization_bm(bm_sacct_df=None, uptime_secs=None, start_date=None, end_date=None, util_method=None):
    global DEBUG_P
    global SECS_PER_DAY

    if DEBUG_P:
        print(f'DEBUG: utilization_bm(): uptime_secs = {uptime_secs} = {uptime_secs / 3600. / 24.} days')
        print(f'DEBUG: utilization_bm(): start_date = {start_date}')
        print(f'DEBUG: utilization_bm():   end_date = {end_date}')

    # need to convert ReqMem field to GiB; values read in are
    # strings with last character being K,M,G,T, etc (why isn't it "k" instead of "K"?)
    bm_sacct_df['ReqMem'] = bm_sacct_df['ReqMem'].apply(convert_to_GiB)

    if not uptime_secs:
        # N.B. this does not take downtime into account
        period_of_interest = end_date - start_date

        # DEBUG
        print(f'DEBUG: utilization_bm(): period_of_interest = {period_of_interest}')

        # no. nodes * mem. per node (~1.5 TiB = 1546 GiB) * tot. seconds
        max_memseconds = 2. * 1546. * period_of_interest.total_seconds()
    else:
        # no. nodes * mem. per node (~1.5 TiB = 1546 GiB) * tot. seconds
        max_memseconds = 2. * 1546. * uptime_secs

    if DEBUG_P:
        print(f'DEBUG: utilization_bm(): max_memseconds = {max_memseconds}')

    if not uptime_secs:
        # N.B. this does not take downtime into account
        period_of_interest = end_date - start_date

        if DEBUG_P:
            print(f'DEBUG utilization_bm(): period_of_interest = {period_of_interest}')

        # no. of nodes * no. of cores per node * tot. days
        max_nodedays = nodedays('bm', period_of_interest.total_seconds())
    else:
        # no. of nodes * no. of cores per node * tot. days
        max_nodedays = nodedays('bm', uptime_secs)

    bm_util = 0.
    if util_method == UtilMethod.BY_RESOURCE:
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

        mem_util = total_memseconds_allocated / max_memseconds * 100.

        print()
        print(f'BIGMEM UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(bm_sacct_df.index):,}')
        print(f'Total avail.: {max_memseconds/SECS_PER_DAY:.5e} GiB-days')
        print(f'Allocated:    {total_memseconds_allocated/SECS_PER_DAY:.5e} GiB-days')
        print(f'Bigmem utilization: {mem_util:.2f} %')

        # summary stats
        print()
        print(f'Mean amount of memory per job: {bm_sacct_df["ReqMem"].mean():7.2f} GiB (std. dev. {bm_sacct_df["ReqMem"].std():7.2f})')
        print(f'Max. amount of memory per job: {bm_sacct_df["ReqMem"].max():7.2f} GiB')
        print(f'Min. amount of memory per job: {bm_sacct_df["ReqMem"].min():7.2f} GiB')

        bm_util = mem_util
    elif util_method == UtilMethod.BY_NODE:
        # create new column for by-node cost: max(fraction of cores, fraction of memory)
        bm_sacct_df['FracNode'] = bm_sacct_df.apply(lambda x: max(x.ReqCPUS / CPUS_PER_NODE['bm'], x.ReqMem / MEM_PER_NODE['bm']), axis=1)

        # create new column for fractional node * time
        bm_sacct_df['FracNodeSeconds'] = bm_sacct_df[['Elapsed', 'FracNode']].product(axis=1)

        if DEBUG_P:
            print(f'DEBUG: utilization_bm(): bm_sacct_df = \n{bm_sacct_df}')

        total_nodedays_allocated = bm_sacct_df['FracNodeSeconds'].sum() / SECS_PER_DAY

        node_util = 0.

        print()
        print(f'BIGMEM NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(bm_sacct_df.index):,}')
        print(f'Total avail.: {max_nodedays:.5e} node-days')
        print(f'Allocated:    {total_nodedays_allocated:.5e} node-days')

        node_util = total_nodedays_allocated / max_nodedays * 100.

        print(f'Node utilization: {node_util:.2f} %')

        # summary stats
        print()
        print(f'Mean amount of memory per job: {bm_sacct_df["ReqMem"].mean():7.2f} GiB (std. dev. {bm_sacct_df["ReqMem"].std():.2f} GiB)')
        print(f'Max. amount of memory per job: {bm_sacct_df["ReqMem"].max():7.2f} GiB')
        print(f'Min. amount of memory per job: {bm_sacct_df["ReqMem"].min():7.2f} GiB')

        bm_util = node_util
    elif util_method == UtilMethod.BY_BILLING:
        max_su = BILLING_PER_NODE['bm'] * max_nodedays
        total_su_allocated = bm_sacct_df['SU'].sum() / HOURS_PER_DAY

        bm_util = 0.

        print()
        print(f'BIGMEM NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(bm_sacct_df.index):,}')
        print(f'Total avail.: {max_su:.5e} SU')
        print(f'Allocated:    {total_su_allocated:.5e} SU')

        su_util = total_su_allocated / max_su * 100.
        print(f'SU utilization: {su_util:.2f} %')

        bm_util = su_util

    return bm_util, bm_sacct_df


def utilization_def(def_sacct_df=None, uptime_secs=None, start_date=None, end_date=None, util_method=None):
    global DEBUG_P
    global SECS_PER_DAY

    if not uptime_secs:
        # N.B. this does not take downtime into account
        period_of_interest = end_date - start_date

        if DEBUG_P:
            print(f'DEBUG utilization_def(): period_of_interest = {period_of_interest}')

        # no. of nodes * no. of cores per node * tot. days
        max_nodedays = nodedays('def', period_of_interest.total_seconds())
    else:
        # no. of nodes * no. of cores per node * tot. days
        max_nodedays = nodedays('def', uptime_secs)


    # create a CPUseconds column
    def_sacct_df['CPUseconds'] = def_sacct_df[['Elapsed', 'ReqCPUS']].product(axis=1)

    def_util = 0.
    if util_method == UtilMethod.BY_RESOURCE:
        max_cpudays = CPUS_PER_NODE['def'] * max_nodedays
        total_cpudays_allocated = def_sacct_df['CPUseconds'].sum() / SECS_PER_DAY

        cpu_util = 0.

        print()
        print(f'CPU UTILIZATION STD. NODES ("def" and "long") ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(def_sacct_df.index):,}')
        print(f'Total avail.: {max_cpudays:.5e} CPU-days')
        print(f'Allocated:    {total_cpudays_allocated:.5e} CPU-days')

        cpu_util = total_cpudays_allocated / max_cpudays * 100.
        print(f'CPU utilization: {cpu_util:.2f} %')

        # summary stats
        print()
        print(f'Mean no. of CPU cores per job: {def_sacct_df["ReqCPUS"].mean():8.2f} (std. dev. {def_sacct_df["ReqCPUS"].std():.2f})')
        print(f'Max. no. of CPU cores per job: {def_sacct_df["ReqCPUS"].max():5,d}')

        def_util = cpu_util
    elif util_method == UtilMethod.BY_NODE:
        # need to convert ReqMem field to GiB; values read in are
        # strings with last character being K,M,G,T, etc (why isn't it "k" instead of "K"?)
        def_sacct_df['ReqMem'] = def_sacct_df['ReqMem'].apply(convert_to_GiB)

        # create new column for by-node cost: max(fraction of cores, fraction of memory)
        def_sacct_df['FracNode'] = def_sacct_df.apply(lambda x: max(x.ReqCPUS / CPUS_PER_NODE['def'], x.ReqMem / MEM_PER_NODE['def']), axis=1)

        # create new column for fractional node * time
        def_sacct_df['FracNodeSeconds'] =  def_sacct_df[['Elapsed', 'FracNode']].product(axis=1)

        total_nodedays_allocated = def_sacct_df['FracNodeSeconds'].sum() / SECS_PER_DAY

        node_util = 0.

        print()
        print(f'STANDARD NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(def_sacct_df.index):,}')
        print(f'Total avail.: {max_nodedays:.5e} node-days')
        print(f'Allocated:    {total_nodedays_allocated:.5e} node-days')

        node_util = total_nodedays_allocated / max_nodedays * 100.
        print(f'Node utilization: {node_util:.2f} %')

        # summary stats
        print()
        print(f'Mean no. of CPU cores per job: {def_sacct_df["ReqCPUS"].mean():8.2f} (std. dev. {def_sacct_df["ReqCPUS"].std():.2f})')
        print(f'Max. no. of CPU cores per job: {def_sacct_df["ReqCPUS"].max():5,d}')

        def_util = node_util
    elif util_method == UtilMethod.BY_BILLING:
        max_su = BILLING_PER_NODE['def'] * max_nodedays
        total_su_allocated = def_sacct_df['SU'].sum() / HOURS_PER_DAY

        def_util = 0.

        print()
        print(f'STANDARD NODE UTILIZATION ({start_date.year}-{start_date.month:02d} -- {end_date.year}-{end_date.month:02d} inclusive)')
        print(f'No. of jobs:  {len(def_sacct_df.index):,}')
        print(f'Total avail.: {max_su:.5e} SU')
        print(f'Allocated:    {total_su_allocated:.5e} SU')

        su_util = total_su_allocated / max_su * 100.
        print(f'SU utilization: {su_util:.2f} %')

        def_util = su_util

    return def_util, def_sacct_df


def utilization(partition='def', sacct_df=None, uptime_secs=None, start_date=None, end_date=None, util_method=UtilMethod.BY_NODE):
    global DEBUG_P

    if DEBUG_P:
        print(f'DEBUG: utilization(): partition = {partition}')
        print('DEBUG: utilization(): sacct_df.head(20) = ')
        print(sacct_df.head(20))
        print(f'DEBUG: utilization(): util_method = {util_method}')
        print(f'DEBUG: utilization(): start_date = {start_date}; end_date = {end_date}')
        print(f'DEBUG: utilization(): All NA rows: {sacct_df[sacct_df.isna().any(axis=1)]}')

    utilization = 0.

    def_sacct_df = sacct_df[(sacct_df['Partition'] == 'def') | (sacct_df['Partition'] == 'long')].copy(deep=True)
    gpu_sacct_df = sacct_df[(sacct_df['Partition'] == 'gpu') | (sacct_df['Partition'] == 'gpulong')].copy(deep=True)
    bm_sacct_df = sacct_df[(sacct_df['Partition'] == 'bm')].copy(deep=True)

    def_sacct_df['NodeType'] = 'standard'
    gpu_sacct_df['NodeType'] = 'gpu'
    bm_sacct_df['NodeType'] = 'bigmem'

    if DEBUG_P:
        print(f'DEBUG: utilization(): def_sacct_df.nunique() = \n{def_sacct_df.nunique()}')
        print(f'DEBUG: utilization(): gpu_sacct_df.nunique() = \n{gpu_sacct_df.nunique()}')
        print(f'DEBUG: utilization():  bm_sacct_df.nunique() = \n{bm_sacct_df.nunique()}')

    if partition == 'def':
        utilization = utilization_def(def_sacct_df, uptime_secs, start_date, end_date, util_method)
    elif partition == 'gpu':
        utilization = utilization_gpu(gpu_sacct_df, uptime_secs, start_date, end_date, util_method)
    elif partition == 'bm':
        utilization = utilization_bm(bm_sacct_df, uptime_secs, start_date, end_date, util_method)

    return utilization


def usage_by_account(def_sacct_df=None, gpu_sacct_df=None, bm_sacct_df=None, uptime_secs=None, start_date=None, end_date=None, util_method=UtilMethod.BY_BILLING):
    global DEBUG_P

    if DEBUG_P:
        print('DEBUG: usage_by_account(): ')
        print('DEBUG: usage_by_account(): describe()')
        print(sacct_df.describe())
        print('DEBUG: usage_by_account(): head(10)')
        print(sacct_df.head(10))
        print('DEBUG: usage_by_account(): tail(10)')
        print(sacct_df.tail(10))
        print(f'DEBUG: usage_by_account(): util_method = {util_method}')
        print()

    # df looks like
    #          JobID          Account Partition   Elapsed  ReqCPUS ReqMem                              ReqTRES                   AllocTRES
    # 0      2707773  cappscmaqnh3prj      long  691223.0      432   576G  billing=432,cpu=432,mem=576G,node=9  billing=432,cpu=432,node=9
    # 4      2708652            huprj      long  630147.0       48    64G     billing=48,cpu=48,mem=64G,node=1    billing=48,cpu=48,node=1
    # 8      2821593     livshultzprj       def   86424.0        1    25G       billing=1,cpu=1,mem=25G,node=1      billing=1,cpu=1,node=1
    # 11     2821594     livshultzprj       def   86414.0        1    25G       billing=1,cpu=1,mem=25G,node=1      billing=1,cpu=1,node=1
    # 14     2821595     livshultzprj       def   86408.0        1    25G       billing=1,cpu=1,mem=25G,node=1      billing=1,cpu=1,node=1
    # 17     2821596     livshultzprj       def   86401.0        1    25G       billing=1,cpu=1,mem=25G,node=1      billing=1,cpu=1,node=1
    # 20     2821619          kwonprj       def   86366.0        4    50G       billing=4,cpu=4,mem=50G,node=1      billing=4,cpu=4,node=1
    # 23     2821646          kwonprj       def   86353.0        4    50G       billing=4,cpu=4,mem=50G,node=1      billing=4,cpu=4,node=1
    # 26  2826716_11       bellamyprj       def   67855.0        4     4G        billing=4,cpu=4,mem=4G,node=1      billing=4,cpu=4,node=1
    # 29  2826716_12       bellamyprj       def   67781.0        4     4G        billing=4,cpu=4,mem=4G,node=1      billing=4,cpu=4,node=1

    start_date_str = f'{start_date.year}{start_date.month:02d}'
    end_date_str = f'{end_date.year}{end_date.month:02d}'

    if util_method == UtilMethod.BY_NODE:
        usage_df = pd.concat([def_sacct_df[['Account', 'NodeType', 'FracNodeSeconds']],
                            gpu_sacct_df[['Account', 'NodeType', 'FracNodeSeconds']],
                            bm_sacct_df[['Account', 'NodeType', 'FracNodeSeconds']]])

        # convert to NodeHours
        usage_df['NodeHours'] = usage_df.apply(lambda row: row.FracNodeSeconds / SECS_PER_HOUR, axis='columns')

        if DEBUG_P:
            print(usage_df[['Account', 'NodeType', 'NodeHours']].groupby(['Account', 'NodeType']).sum())

        usage_df[['Account', 'NodeType', 'NodeHours']].groupby(['Account', 'NodeType']).sum().reset_index().to_csv(f'usage_by_account_{start_date_str}_{end_date_str}.csv')

        usage_df['TotalNodeHours'] = usage_df[['Account', 'NodeType', 'NodeHours']].groupby('Account')['NodeHours'].transform(sum)

        if DEBUG_P:
            print(usage_df[['Account', 'TotalNodeHours']].drop_duplicates().sort_values(by=['TotalNodeHours'], ascending=False))

        # save CSV
        usage_df[['Account', 'TotalNodeHours']].drop_duplicates().sort_values(by=['TotalNodeHours'], ascending=False).to_csv(f'usage_by_account_totals_{start_date_str}_{end_date_str}.csv')

        usage_df = usage_df[['Account', 'NodeType', 'NodeHours']].groupby(['Account', 'NodeType']).sum().reset_index()
        usage_df = usage_df.pivot(index='Account', columns='NodeType', values='NodeHours').fillna(0)

        # save CSV
        usage_df.to_csv(f'usage_by_account_per_nodetype_{start_date_str}_{end_date_str}.csv')

        usage_df.columns = [''.join(str(s).strip() for s in col if s) for col in usage_df.columns]
        usage_df.reset_index(inplace=True)
        usage_df['TotalNodeHours'] = usage_df['bigmem'] + usage_df['gpu'] + usage_df['standard']
        usage_df['GPUStandardHours'] = usage_df['gpu'] + usage_df['standard']
        usage_df.sort_values(by=['TotalNodeHours'], ascending=False, inplace=True)
        # want only groups which used more than 1 node-hour per month
        delta_t = Delorean(end_date, timezone='UTC') - Delorean(start_date, timezone='UTC')
        n_months = int(delta_t.total_seconds() / SECS_PER_DAY / 30.)
        output_df = usage_df.query(f'TotalNodeHours > {n_months}').sort_values(by=['TotalNodeHours'], ascending=False)

        if DEBUG_P:
            print('DEBUG: output_df = ')
            print(output_df.head(5))
            print(output_df[output_df['Account'] == 'urbancmriprj'])
            output_df.to_csv('foo.csv')

        # seaborn plot
        # 3 bars: bigmem, gpu, standard
        sns.set_theme()  # set seqborn defaults
        sns.set_style('whitegrid')
        sns.color_palette('colorblind')
        sns.set(rc={'figure.figsize': (10., 7.5)})
        sns.set_context('paper', rc={'font.size': 8})

        paper_dims = (11., 8.5)
        fig, ax = plt.subplots(figsize=paper_dims)

        # stack the bars right to left (layered bottom to top)
        # first one is bigmem
        sns.barplot(ax=ax, data=output_df, x='TotalNodeHours', y='Account', color='steelblue')
        # second one is gpu
        sns.barplot(ax=ax, data=output_df, x='GPUStandardHours', y='Account', color='olivedrab')
        # third one is standard
        sns.barplot(ax=ax, data=output_df, x='standard', y='Account', color='gold')
        bigmem_bar = mpatches.Patch(color='steelblue', label='bigmem')
        gpu_bar = mpatches.Patch(color='olivedrab', label='gpu')
        standard_bar = mpatches.Patch(color='gold', label='standard')
        plt.legend(handles=[standard_bar, gpu_bar, bigmem_bar], loc='lower right')
        plt.xlabel('Node-Hours')
        plt.ylabel('Account')
        title_font = {'color': 'black', 'weight': 'bold', 'size': 14}
        plt.title(f'Picotte node compute usage by account (>1 node-hr) {start_date:%b %Y} to {end_date:%b %Y}', fontdict=title_font)
        plt.savefig(f'node_usage_by_account_per_nodetype_{start_date_str}_{end_date_str}.png', dpi=300)
        plt.clf()
    elif util_method == UtilMethod.BY_RESOURCE:
        print('No account utilization by resource')
    elif util_method == UtilMethod.BY_BILLING:
        usage_df = pd.concat([def_sacct_df[['Account', 'NodeType', 'SU']],
                            gpu_sacct_df[['Account', 'NodeType', 'SU']],
                            bm_sacct_df[['Account', 'NodeType', 'SU']]])

        if DEBUG_P:
            print('DEBUG: usage_by_account(): usage_df')
            print(usage_df.describe())
            print(usage_df.head(10))

        usage_df['TotalSU'] = usage_df[['Account', 'NodeType', 'SU']].groupby('Account')['SU'].transform(sum)
        usage_df = usage_df[['Account', 'NodeType', 'SU']].groupby(['Account', 'NodeType']).sum().reset_index()

        total_su_per_account = usage_df[['Account', 'SU']].groupby(['Account']).sum().reset_index()

        with open(f'total_su_per_account_{start_date_str}_{end_date_str}.csv', 'w') as f:
            total_su_per_account.to_csv(f)

        usage_df = usage_df.pivot(index='Account', columns='NodeType', values='SU').fillna(0)
        usage_df.columns = [''.join(str(s).strip() for s in col if s) for col in usage_df.columns]
        usage_df.reset_index(inplace=True)
        usage_df['TotalSU'] = usage_df['bigmem'] + usage_df['gpu'] + usage_df['standard']
        usage_df['GPUSU'] = usage_df['gpu'] + usage_df['standard']
        usage_df.sort_values(by=['TotalSU'], ascending=False, inplace=True)

        # want only groups which used more than 48 SU
        output_df = usage_df.query(f'TotalSU > 48').sort_values(by=['TotalSU'], ascending=False)

        # seaborn plot
        # 3 bars: bigmem, gpu, standard
        sns.set_theme()  # set seqborn defaults
        sns.set_style('whitegrid')
        sns.color_palette('colorblind')
        sns.set(rc={'figure.figsize': (10., 7.5)})
        sns.set_context('paper', rc={'font.size': 8})

        paper_dims = (11., 8.5)
        fig, ax = plt.subplots(figsize=paper_dims)

        # stack the bars right to left (layered bottom to top)
        # first one is bigmem
        sns.barplot(ax=ax, data=output_df, x='TotalSU', y='Account', color='steelblue')
        # second one is gpu
        sns.barplot(ax=ax, data=output_df, x='GPUSU', y='Account', color='olivedrab')
        # third one is standard
        sns.barplot(ax=ax, data=output_df, x='standard', y='Account', color='gold')
        bigmem_bar = mpatches.Patch(color='steelblue', label='bigmem')
        gpu_bar = mpatches.Patch(color='olivedrab', label='gpu')
        standard_bar = mpatches.Patch(color='gold', label='standard')
        plt.legend(handles=[standard_bar, gpu_bar, bigmem_bar], loc='lower right')
        plt.xlabel('SU')
        plt.ylabel('Account')
        title_font = {'color': 'black', 'weight': 'bold', 'size': 14}
        plt.title(f'Picotte compute SU usage by account (>48 SU) {start_date:%b %Y} to {end_date:%b %Y}', fontdict=title_font)
        plt.savefig(f'su_usage_by_account_per_nodetype_{start_date_str}_{end_date_str}.png', dpi=300)
        plt.clf()

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
    parser.add_argument('-D', '--data-dir', required=True, help='Directory containing sacct output files')
    parser.add_argument('-u', '--utilization-method', type=UtilMethod, choices=list(UtilMethod), help='Method used to compute utilization')

    args = parser.parse_args()

    DEBUG_P = args.debug

    util_method = args.utilization_method

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
        print(f'DEBUG: main(): dates = {dates}')
        print(f'DEBUG: main(): date_strings = {date_strings}')

    # List of partitions
    partitions = ['def', 'gpu', 'bm']

    # Generate list of filenames
    filenames = []
    for p in partitions:
        for dstr in date_strings:
            filenames.append(f'{args.data_dir}/sacct_{p}_{dstr}.csv')

    if DEBUG_P:
        for f in filenames:
            print(f'DEBUG: main(): filenames : {f}')

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

    # N.B. sorting not necessary to drop duplicates

    if DEBUG_P:
        print(f'DEBUG: main(): no. of duplicated rows: {len(sacct_df[sacct_df.duplicated(subset=["JobID"], keep=False)].index)}')

    # drop duplicates
    sacct_df.drop_duplicates(subset=['JobID'], inplace=True, ignore_index=True)

    if DEBUG_P:
        print(f'DEBUG: main(): no. of duplicated rows after drop_duplicates(): {len(sacct_df[sacct_df.duplicated(subset=["JobID"], keep=False)].index)}')

    # format the Elapsed field
    sacct_df['Elapsed'].replace(to_replace=r'\-', value=' days ', regex=True, inplace=True)

    # convert Elapsed column to seconds
    sacct_df.loc[:, 'Elapsed'] = pd.to_timedelta(sacct_df['Elapsed'])
    sacct_df.loc[:, 'Elapsed'] = sacct_df['Elapsed'].dt.total_seconds()

    sacct_df = sacct_df[['JobID', 'Account', 'User', 'Partition', 'Elapsed', 'ReqCPUS', 'ReqMem', 'ReqTRES', 'AllocTRES']]

    # add Billing column
    sacct_df['Billing'] = sacct_df['AllocTRES'].str.extract(r'billing=(\d+)')
    sacct_df['Billing'] = pd.to_numeric(sacct_df['Billing'])

    # create SU column = Billing * Elapsed (in hours)
    sacct_df['SU'] = sacct_df[['Elapsed', 'Billing']].product(axis='columns')
    sacct_df['SU'] = sacct_df['SU'] / SECS_PER_HOUR

    sacct_df.to_pickle(f'sacct_df_{date_strings[0]}_{date_strings[-1]}.pkl')

    if DEBUG_P:
        print('DEBUG: utilization(): INFO')
        print(sacct_df.info())

    util = {}
    util['general'], uptime_secs = sreport_utilization(start_date, end_date)
    util['def'], def_sacct_df = utilization('def', sacct_df=sacct_df, uptime_secs=uptime_secs, start_date=start_date, end_date=end_date, util_method=util_method)
    util['gpu'], gpu_sacct_df = utilization('gpu', sacct_df=sacct_df, uptime_secs=uptime_secs, start_date=start_date, end_date=end_date, util_method=util_method)
    util['bm'], bm_sacct_df = utilization('bm', sacct_df=sacct_df, uptime_secs=uptime_secs, start_date=start_date, end_date=end_date, util_method=util_method)
    print()

    if DEBUG_P:
        print(f'DEBUG: util = {util}')

    usage_by_account(def_sacct_df=def_sacct_df, gpu_sacct_df=gpu_sacct_df, bm_sacct_df=bm_sacct_df, uptime_secs=uptime_secs, start_date=start_date, end_date=end_date, util_method=util_method)

if __name__ == '__main__':
    main()

