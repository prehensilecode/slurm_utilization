#!/usr/bin/env python3
#    utilization_report.py - Compute utilization by reading sreport usage and normalizing against total available SUs.
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
import subprocess
import decimal
from decimal import Decimal
import datetime
import calendar
import delorean


debug_p = True

rate = Decimal('0.0123')

hrs_per_day = 24

su_per_core_hour = 1
su_per_gpu_hour = 43
su_per_tib_hour = 68

num_def_nodes = 74
def_nodes_cores_per_node = 48

num_gpu_nodes = 12
gpu_nodes_gpus_per_node = 4

num_bm_nodes = 2
bm_nodes_mem_per_node = 1546000  # MiB


def su_days_def(n_days):
    global num_def_nodes
    global def_nodes_cores_per_node
    global su_per_core_hour
    global hrs_per_day

    return num_def_nodes * n_days * hrs_per_day * su_per_core_hour


def su_days_gpu(n_days):
    global num_gpu_nodes
    global gpu_nodes_gpus_per_node
    global su_per_gpu_hour
    global hrs_per_day

    return num_gpu_nodes * gpu_nodes_gpus_per_node * n_days * hrs_per_day * su_per_gpu_hour


def su_days_bigmem(n_days):
    global num_bm_nodes
    global bm_nodes_mem_per_node
    global su_per_tib_hour
    global hrs_per_day

    mib_to_tib = 1024 * 1024

    return num_bm_nodes * bm_nodes_mem_per_node * n_days * hrs_per_day * su_per_tib_hour / mib_to_tib


def main():
    global debug_p
    global rate
    global num_def_nodes
    global def_nodes_cores_per_node
    global num_gpu_nodes
    global gpu_nodes_gpus_per_node
    global num_bm_nodes
    global bm_nodes_mem_per_node

    year = 2021
    months = [2, 3, 4, 5]

    print('Utilization by SUs')
    print('------------------')
    print('')
    overall_sus = 0.
    overall_utilized_sus = 0.
    for month in months:
        n_days = calendar.monthrange(year, month)[1]
        date = datetime.date(year, month, 1)
        date_str = date.strftime('%b %Y')
        print(f'{date_str} ({n_days} days)')
        def_sus = su_days_def(n_days)
        print(f'{def_sus:8.6e} std. SUs')
        gpu_sus = su_days_gpu(n_days)
        print(f'{gpu_sus:8.6e} GPU SUs')
        bm_sus = su_days_bigmem(n_days)
        print(f'{bm_sus:8.6e} bigmem SUs')

        total_sus = def_sus + gpu_sus + bm_sus
        overall_sus += total_sus
        print(f'{total_sus:50.6e} total SUs')

        command = f'sreport -n -P cluster AccountUtilizationByUser Account=root Tree Start={year}-{month:02}-01 End={year}-{month:02}-{n_days:02} -T billing'.split(' ')
        sreport = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split('\n')

        total_sus_utilized = float(sreport[0].split('|')[5]) / 60.
        overall_utilized_sus += total_sus_utilized

        print(f'{total_sus_utilized:50.6e} utilized SUs')
        print(f'                                      -------------------------')
        print(f'                                      Utilization = {total_sus_utilized/total_sus*100.:5.2f}%')

    print('')
    print(f'TOTAL AVAILABLE SUs: {overall_sus:12.6e}')
    print(f'TOTAL UTILIZED SUs:  {overall_utilized_sus:12.6e}')
    print(f'                     ------------')
    print(f'       UTILIZATION:  {overall_utilized_sus/overall_sus*100.:5.2f}%')


if __name__ == '__main__':
    main()
