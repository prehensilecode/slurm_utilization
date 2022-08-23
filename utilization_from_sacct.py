#!/usr/bin/env python3
import sys
import os
import pandas


def read_sacct(filename):
    sacct_data = pandas.read_csv(filename, delimiter='|')

    print('Info')
    sacct_data.info()

    print('')

    print('Description')
    print(sacct_data.describe())


def main():
    read_sacct('Data/sacct.csv')


if __name__ == '__main__':
    main()

