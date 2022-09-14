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
from decimal import Decimal
from datetime import datetime, date, timedelta
from delorean import Delorean
import calendar
import fiscalyear
import argparse


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
#bm_nodes_mem_per_node = 1546000  # MiB
bm_nodes_mem_per_node = 1.5  # TiB


def su_days_def(n_days):
    global num_def_nodes
    global def_nodes_cores_per_node
    global su_per_core_hour
    global hrs_per_day

    return num_def_nodes * def_nodes_cores_per_node * n_days * hrs_per_day * su_per_core_hour


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

    gib_to_tib = 1024.

    return num_bm_nodes * bm_nodes_mem_per_node * n_days * hrs_per_day * su_per_tib_hour


def pretty_print(start_date, end_date, total_su, alloc_su, total_down_su, idle_su):
    print('<table>')
    print(' <tr>')
    print(f'  <th colspan="4">Utilization for period: {start_date} -- {end_date}')
    print(' </tr>')
    print(' <tr>')
    print('  <td>Total SUs:</td>')
    print(f' <td align="right">{total_su:9.6e}</td>')
    print('  <td>&nbsp;</td>')
    print('  <td>&nbsp;</td>')
    print(' </tr>')
    print(' <tr>')
    print('  <td>Utilized SUs:</td>')
    print(f' <td align="right">{alloc_su:9.6e}</td>')
    print('  <td>Percent utilization:</td>')
    print(f' <td align="right">{alloc_su/total_su*100.:5.2f}</td>')
    print(' </tr>')
    print(' <tr>')
    print('  <td>Downtime SUs:</td>')
    print(f' <td align="right">{total_down_su:9.6e}</td>')
    print('  <td>Percent downtime:</td>')
    print(f' <td align="right">{total_down_su/total_su*100.:5.2f}</td>')
    print(' </tr>')
    print(' <tr>')
    print('  <td>Idle SUs:</td>')
    print(f' <td>{idle_su:9.6e}</td>')
    print('  <td>Percent idle time:</td>')
    print(f' <td>{idle_su/total_su*100.:5.2f}</td>')
    print(' </tr>')
    print('</table>')


def manual_utilization():
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
        date = date(year, month, 1)
        dt = timedelta(days=1)
        end_period = date(year, month, n_days) + dt
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

        command = f'sreport -n -P cluster AccountUtilizationByUser Account=root Tree Start={year}-{month:02}-01 End={end_period.year}-{end_period.month:02}-01 -T billing'.split(' ')

        print(f'command = {command}')
        sreport = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split('\n')

        total_sus_utilized = float(sreport[0].split('|')[5]) / 60.
        overall_utilized_sus += total_sus_utilized

        print(f'{total_sus_utilized:50.6e} utilized SUs')
        print(f'                                      -------------------------')
        print(f'                                      Utilization = {total_sus_utilized/total_sus*100.:5.2f} %')

    print('')
    print(f'TOTAL AVAILABLE SUs: {overall_sus:12.6e}')
    print(f'TOTAL UTILIZED SUs:  {overall_utilized_sus:12.6e}')
    print(f'                     ------------')
    print(f'       UTILIZATION:  {overall_utilized_sus/overall_sus*100.:5.2f}%')


def sreport_utilization_fy(year=None, output_p=True, pretty_print_p=False):
    global debug_p

    # cumulative utilization for current FY
    if not year:
        today = date.today()
        year = today.year

    fiscalyear.setup_fiscal_calendar(start_month=7)  # Drexel FY starts July 1

    fy = fiscalyear.FiscalYear(year)
    next_fy = fiscalyear.FiscalYear(year+1)

    if debug_p:
        print(f'DEBUG: fy = {fy}')
        print(f'DEBUG:      {fy.start} {type(fy.start)}')
        print(f'DEBUG:      {fy.end} {type(fy.end)}')
        print(f'DEBUG: next_fy = {next_fy}')
        print(f'DEBUG:           {next_fy.start} {type(next_fy.start)}')
        print(f'DEBUG:           {next_fy.end} {type(next_fy.end)}')
        print('')

    min_per_hour = 60.

    command = f'sreport -n -P cluster utilization -T billing start={fy.start.year}-{fy.start.month:02}-01 end={next_fy.start.year}-{next_fy.start.month:02}-01'.split(' ')

    if debug_p:
        print(f'DEBUG: command = {command}')
    sreport = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split('|')
    alloc_su = float(sreport[2]) / min_per_hour
    down_su = float(sreport[3]) / min_per_hour
    planned_down_su = float(sreport[4]) / min_per_hour
    total_down_su = down_su + planned_down_su
    idle_su = float(sreport[5]) / min_per_hour
    reserved_su = float(sreport[6]) / min_per_hour
    total_su = float(sreport[7]) / min_per_hour

    today = Delorean()
    fy_start = Delorean(fy.start, timezone='US/Eastern')
    next_fy_start = Delorean(next_fy.start, timezone='US/Eastern')
    days_elapsed = (today - fy_start).days
    days_in_fy = (next_fy_start - fy_start).days

    if output_p:
        if pretty_print_p:
            print('NICE FORMAT')
        else:
            print(f'CUMULATIVE UTILIZATION REPORT for fiscal year starting {fy.start.year}-{fy.start.month:02}-01')
            if today < next_fy_start:
                print(f'{days_elapsed} days out of {days_in_fy} ({days_elapsed / days_in_fy * 100:.02f}%)')
            print('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')
            print(f'Total SUs:     {total_su:9.6e}')
            print(f'Utilized SUs:  {alloc_su:9.6e}      Percent utilization:   {alloc_su/total_su*100.:5.2f} %')
            print(f'Downtime SUs:  {total_down_su:9.6e}      Percent down time:     {total_down_su/total_su*100.:5.2f} %')
            print(f'Idle SUs:      {idle_su:9.6e}      Percent idle time:     {idle_su/total_su*100.:5.2f} %')
            print('')


def sreport_utilization_year_month(year, month, output_p=True, pretty_print_p=False):
    global debug_p
    global rate
    global num_def_nodes
    global def_nodes_cores_per_node
    global num_gpu_nodes
    global gpu_nodes_gpus_per_node
    global num_bm_nodes
    global bm_nodes_mem_per_node

    # sreport -n -P -T billing ...
    # Cluster|TRES Name|Allocated|Down|PLND Down|Idle|Reserved|Reported
    # picotte|billing|13033854|1879748|0|227783998|0|242697600

    n_days = calendar.monthrange(year, month)[1]
    date_of_interest = date(year, month, 1)
    last_day = date(year, month, n_days)
    period_end = last_day + timedelta(days=1)

    date_str = date_of_interest.strftime('%b %Y')

    date_hdr_str = None
    today = date.today()
    if year == today.year and month == today.month:
        if today.day < n_days:
            date_hdr_str = f'{date_str} ({today.day} out of {n_days} days)'
    else:
        date_hdr_str = f'{date_str}'

    if output_p:
        print('UTILIZATION REPORT')
        print(date_hdr_str)
        print('- - - - - - - - - - - - - -')

    min_per_hour = 60.
    command = f'sreport -n -P cluster utilization -T billing start={year}-{month:02}-01 end={period_end.year}-{period_end.month:02}-01'.split(' ')

    if debug_p:
        print(f'DEBUG: command = {command}')

    sreport = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split('|')

    if debug_p:
        print(f'DEBUG: sreport = {sreport}')

    alloc_su = float(sreport[2])/min_per_hour
    down_su = float(sreport[3])/min_per_hour
    planned_down_su = float(sreport[4])/min_per_hour
    total_down_su = down_su + planned_down_su
    idle_su = float(sreport[5])/min_per_hour
    reserved_su = float(sreport[6])/min_per_hour
    total_su = float(sreport[7])/min_per_hour

    if output_p:
        print(f'Total SUs:     {total_su:9.6e}')
        print(f'Utilized SUs:  {alloc_su:9.6e}      Percent utilization:   {alloc_su/total_su*100.:5.2f} %')
        print(f'Downtime SUs:  {total_down_su:9.6e}      Percent down time:     {total_down_su/total_su*100.:5.2f} %')
        print(f'Idle SUs:      {idle_su:9.6e}      Percent idle time:     {idle_su/total_su*100.:5.2f} %')
        print('')


def sreport_utilization(start_date, end_date, output_p=True, pretty_print_p=False):
    global debug_p
    global rate
    global num_def_nodes
    global def_nodes_cores_per_node
    global num_gpu_nodes
    global gpu_nodes_gpus_per_node
    global num_bm_nodes
    global bm_nodes_mem_per_node

    # sreport -n -P -T billing ...
    # Cluster|TRES Name|Allocated|Down|PLND Down|Idle|Reserved|Reported
    # picotte|billing|13033854|1879748|0|227783998|0|242697600

    min_per_hour = 60.
    command = f'sreport -n -P cluster utilization -T billing start={start_date} end={end_date}'.split(' ')
    sreport = subprocess.run(command, check=True, capture_output=True, text=True).stdout.split('|')
    alloc_su = float(sreport[2])/min_per_hour
    down_su = float(sreport[3])/min_per_hour
    planned_down_su = float(sreport[4])/min_per_hour
    total_down_su = down_su + planned_down_su
    idle_su = float(sreport[5])/min_per_hour
    reserved_su = float(sreport[6])/min_per_hour
    total_su = float(sreport[7])/min_per_hour

    if output_p:
        if pretty_print_p:
            pretty_print(start_date, end_date, total_su, alloc_su, total_down_su, idle_su)
        else:
            print(f'Utilization for period: {start_date} -- {end_date}')
            print(f'Total SUs:     {total_su:9.6e}')
            print(f'Utilized SUs:  {alloc_su:9.6e}      Percent utilization:   {alloc_su/total_su*100.:5.2f} %')
            print(f'Downtime SUs:  {total_down_su:9.6e}      Percent down time:     {total_down_su/total_su*100.:5.2f} %')
            print(f'Idle SUs:      {idle_su:9.6e}      Percent idle time:     {idle_su/total_su*100.:5.2f} %')
            print('')


def main():
    global debug_p

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Debugging output')
    parser.add_argument('-s', '--start', default=None,
                        help='Start date for reporting in format YYYY-MM-DD (default: first day of current year-month)')
    parser.add_argument('-e', '--end', default=None,
                        help='End date for reporting in format YYYY-MM-DD (default: last day of current year-month)')
    parser.add_argument('-c', '--cumulative', action='store_true',
                        help='Show cumulative utilization for current fiscal year (default: False)')
    parser.add_argument('-p', '--pretty-print', action='store_true',
                        help='Output in HTML')
    args = parser.parse_args()

    debug_p = args.debug

    if debug_p:
        print('DEBUG: args =', args)

    commission_date = datetime(2021, 2, 1)

    today = date.today()

    start_year = 0
    start_month = 0
    start_day = 0
    if args.start:
        start_year, start_month, start_day = [int(x) for x in args.start.split('-')]
    else:
        start_year, start_month = today.year, today.month
        start_day = 1

    start_date = date(start_year, start_month, start_day)

    end_year = 0
    end_month = 0
    end_day = 0
    if args.cumulative:
        sreport_utilization_fy(start_year)
    else:
        if args.end:
            end_year, end_month, end_day = [int(x) for x in args.end.split('-')]
            end_date = date(end_year, end_month, end_day)

            sreport_utilization(start_date, end_date, pretty_print_p=args.pretty_print)

            if debug_p:
                print(f'DEBUG: start_date = {start_date}; end_date = {end_date}')
        else:
            sreport_utilization_year_month(start_year, start_month)



if __name__ == '__main__':
    main()
