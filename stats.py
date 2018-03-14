#!/usr/bin/env python
# -*- coding: utf-8 -*-

' a cluster condor statistics module '

__author__ = 'Cheng Tu'

import sys
import numpy as np
import re
import math
import datetime as dt
from datetime import timedelta
import matplotlib.pyplot as plt

def add_time(t1, t2):
    return t1+t2

class ReadFile:

    def __init__(self, file_path):
        self.file_path = file_path
        self.line_list = []
        self.line_set = set()
        f = open(self.file_path, 'r')
        for line in f:
            self.line_list.append(line)
            self.line_set.add(line)
        return

    def get_contents_list(self):
        return self.line_list

    def get_contents_set(self):
        return self.line_set

    def show_contents(self):
        print 'Contents in File ', self.file_path
        print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        for line in self.line_list:
            print line
        print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        print 'File End'

class Stats:
    def __init__(self, file_contents):
        self.classname = 'Stats'
        self.file_contents = file_contents
        return

    def decode_line(self, line):
        line_parts = re.sub('\s+', ' ',line)
        line_parts = line_parts.split(' ')
        line_dic = {}
        i_CX = None
        for i, part in enumerate(line_parts):
            if (part == 'C') or (part == 'X'):
                i_CX = i

        if i_CX is None:
            print 'No \'ST\' In This Line: ', line
            return line_dic
        else:
            line_dic['ST']    = line_parts[i_CX]
            line_dic['ID']    = line_parts[0]
            line_dic['OWNER'] = line_parts[1]

            # SUBMITTED
            if self.decode_date_from_str(line_parts[2], line_parts[3]) == False:
                print '\'SUBMITTED\' Of This Line Cannot Be Decoded: ', line
            else:
                line_dic['SUBMITTED'] = self.decode_date_from_str(line_parts[2], line_parts[3])

            # RUNTIME
            if self.decode_runtime_from_str(line_parts[4]) == False:
                print '\'RUNTIME\' Of This Line Cannot Be Decoded: ', line
            else:
                line_dic['RUNTIME'] = self.decode_runtime_from_str(line_parts[4])

            # COMPLETED
            if self.decode_date_from_str(line_parts[i_CX+1], line_parts[i_CX+2]) == False:
                print '\'COMPLETED\' Of This Line Cannot Be Decoded: ', line
                return line_dic
            else:
                line_dic['COMPLETED'] = self.decode_date_from_str(line_parts[i_CX+1], line_parts[i_CX+2])

                # STARTED
                if 'RUNTIME' in line_dic:
                    line_dic['STARTED'] = line_dic['COMPLETED'] - line_dic['RUNTIME']

                    # WAITTIME
                    if 'SUBMITTED' in line_dic:
                        line_dic['WAITTIME'] = line_dic['STARTED'] - line_dic['SUBMITTED']

            return line_dic

    def find_minmax_SUBMITTED(self):
        self.min_SUBMITTED = dt.datetime.max
        self.max_SUBMITTED = dt.datetime.min
        for time in self.decoded_line_list:
            if 'SUBMITTED' in time:
                if time['SUBMITTED'] < self.min_SUBMITTED:
                    self.min_SUBMITTED = time['SUBMITTED']
                if time['SUBMITTED'] > self.max_SUBMITTED:
                    self.max_SUBMITTED = time['SUBMITTED']
        print 'min_SUBMITTED:'
        print self.min_SUBMITTED
        print 'max_SUBMITTED:'
        print self.max_SUBMITTED
        return

    def find_minmax_COMPLETED(self):
        self.min_COMPLETED = dt.datetime.max
        self.max_COMPLETED = dt.datetime.min
        for time in self.decoded_line_list:
            if 'COMPLETED' in time:
                if time['COMPLETED'] < self.min_COMPLETED:
                    self.min_COMPLETED = time['COMPLETED']
                if time['COMPLETED'] > self.max_COMPLETED:
                    self.max_COMPLETED = time['COMPLETED']
        print 'min_COMPLETED:'
        print self.min_COMPLETED
        print 'max_COMPLETED:'
        print self.max_COMPLETED
        return

    def decode_date_from_str(self, daymon, hourmin):
        fname = 'decode_date_from_str'
        if len(daymon.split('/')) == 2 and len(hourmin.split(':')) == 2:
            mon, day = map(int, daymon.split('/') )
            hour, min = map(int, hourmin.split(':'))
            current_year = dt.datetime.now().year
            return dt.datetime(year=current_year, month=mon, day=day, hour=hour, minute=min)
        else:
            print self.classname, ':: ',fname, ':: ', 'Date Cannot be Decoded with Str: ', daymon, ' ', hourmin
            return False

    def decode_all(self, *skip_lines):
        self.decoded_line_list = []
        for i, line in enumerate(self.file_contents):
            if i in skip_lines:
                continue
            self.decoded_line_list.append(self.decode_line(line))
        return

    def avg_runtime(self):
        try:
            self.decoded_line_list
        except NameError:
            print 'No decoded_line_list. Run decode_all First'
            return
        else:
            r_l = []
            for i in range(len(self.decoded_line_list)):
                if 'RUNTIME' in self.decoded_line_list[i]:
                    r_l.append(self.decoded_line_list[i]['RUNTIME'])
            return reduce(add_time, r_l) / len(r_l), len(r_l)

    def avg_waittime(self, type='h'):
        try:
            self.decoded_line_list
        except NameError:
            print 'No decoded_line_list. Run decode_all First'
            return
        else:
            wr_l = []
            for i in range(len(self.decoded_line_list)):
                if 'WAITTIME' in self.decoded_line_list[i]:
                    wr_l.append(self.decoded_line_list[i]['WAITTIME'])
            return reduce(add_time, wr_l) / len(wr_l), len(wr_l)


    def decode_runtime_from_str(self, runtime):
        fname = 'decode_runtime_from_str'
        if len(runtime.split('+')) == 2:
            day, hms = runtime.split('+')
        else:
            print self.classname, ':: ',fname, ':: ', 'Date Cannot be Decoded with Str: ', runtime
            return False
        if len(hms.split(':')) == 3:
            h, m, s = hms.split(':')
        else:
            return False
        day, h, m, s = map(int, [day, h, m, s])
        td = timedelta(days=day, hours=h, minutes=m, seconds=s)
        return td

    def get_num_jobs(self, fromt, tot, type ='h'):
        if type == 'h':
            tstart = ignore_minutes(fromt)
            tend   = ignore_minutes(tot)
            trange = int((tend - tstart).total_seconds() / 3600)
            self.ttable = [tstart+x*ONE_HOUR for x in range(trange)]
            self.num_jobs_list = [0 for n in range(trange)]
            for line in self.decoded_line_list:
                if ('COMPLETED' in line) and ('STARTED' in line):
                    if line['STARTED'] > tstart:
                        job_start = ignore_minutes(line['STARTED'])
                        job_comp  = ignore_minutes(line['COMPLETED'])
                        offset_start = int((job_start - tstart).total_seconds() / 3600)
                        offset_end   = int((job_comp  - tstart).total_seconds() / 3600)
                        for i in range(offset_start, min(offset_end, trange)):
                            self.num_jobs_list[i] += 1
            return np.average(self.num_jobs_list)
        return

    def plt_num_jobs(self, type ='h'):
        try:
            self.ttable
        except NameError:
            print 'No time table. Run get_num_jobs First'
        else:
            if type == 'h':
                #plt.plot(self.ttable, self.num_jobs_list, 'lightskyblue')
                plt.title('Number of Running Jobs Per Hour\n '
                          + str(self.ttable[0]) + ' -> ' + str(self.ttable[-1]))
                plt.fill_between(self.ttable, self.num_jobs_list, color='g', alpha = 0.3)
                plt.show()
        return

    def plt_jobs_rank(self, fromt, tot):
        job_dic = {}
        for line in self.decoded_line_list:
            if ('COMPLETED' in line) and ('STARTED' in line):
                if line['STARTED'] > fromt and line['STARTED'] < tot:
                    job_dic[line['OWNER']] = job_dic.get(line['OWNER'], 0) + 1
        lists = sorted(job_dic.items(), key=lambda x: x[1], reverse=True)
        xname, y = zip(*lists)
        plt.bar(np.arange(len(xname)),y, width = 0.5, align='center', color = 'lightskyblue', edgecolor = 'white')
        plt.xticks(np.arange(len(xname)), xname,rotation=-30)
        plt.ylabel('Number of Jobs')
        plt.xlabel('Username')
        plt.title('Rank: Total Number of Jobs\n' + str(fromt) + ' -> ' + str(tot))
        plt.show()
        return

    def plt_runtime_rank(self, fromt, tot):
        runtime_dic = {}
        for line in self.decoded_line_list:
            if ('COMPLETED' in line) and ('STARTED' in line):
                if line['STARTED'] > fromt and line['STARTED'] < tot:
                    runtime_dic[line['OWNER']] = runtime_dic.get(line['OWNER'], timedelta()) \
                                                 + (min(line['COMPLETED'], tot) - line['STARTED'])
        lists = sorted(runtime_dic.items(), key=lambda x: x[1], reverse=True)
        xname, ydelta = zip(*lists)
        yse = map(to_hours, ydelta)
        plt.bar(np.arange(len(xname)),yse, width = 0.5, align='center', facecolor = 'lightskyblue', edgecolor = 'white')
        plt.xticks(np.arange(len(xname)), xname,rotation=-30)
        plt.ylabel('Hours')
        plt.xlabel('Username')
        plt.title('Rank: Total Runtime\n' + str(fromt) + ' -> ' + str(tot))
        plt.show()
        return


def ignore_minutes(time):
    return dt.datetime(year=time.year, month= time.month, day=time.day, hour=time.hour)

def to_seconds(datedelta):
    return datedelta.total_seconds()

def to_minutes(datedelta):
    return datedelta.total_seconds()/60.0

def to_hours(datedelta):
    return datedelta.total_seconds()/3600.0

ONE_HOUR = timedelta(hours=1)



if __name__ == "__main__":
    HIS_FILE = 'condor_his'
    OUT_FILE = 'condor_stats'
    a = ReadFile(HIS_FILE)
    astats = Stats(a.line_set)
    astats.decode_all(0)
    astats.find_minmax_SUBMITTED()
    astats.find_minmax_COMPLETED()
    print 'One Hour:', ONE_HOUR
    nja = astats.get_num_jobs(astats.min_COMPLETED, astats.max_SUBMITTED)

    outf = open(OUT_FILE, 'w')
    outf.write('Average Running Time: ' + str(astats.avg_runtime()[0]) +
               '. Based On ' + str(astats.avg_runtime()[1]) + ' Jobs' +
               ' From ' + str(astats.min_SUBMITTED) + ' To ' + str(astats.max_COMPLETED) + '\n')
    outf.write('Average Waiting Time: ' + str(astats.avg_waittime()[0]) +
               '. Based On ' + str(astats.avg_runtime()[1]) + ' Jobs' +
               ' From ' + str(astats.min_SUBMITTED) + ' To ' + str(astats.max_COMPLETED) + '\n')
    outf.write('Average Number of Jobs Per Hour: ' + str(int(nja)) + '.' +
               ' From ' + str(astats.min_COMPLETED) + ' To ' + str(astats.max_SUBMITTED) + '\n')
    outf.close()

    astats.plt_num_jobs()
    print astats.avg_runtime()[0]
    print astats.avg_waittime()[0]
    astats.plt_jobs_rank(astats.min_COMPLETED, astats.max_SUBMITTED)
    astats.plt_runtime_rank(astats.min_COMPLETED, astats.max_SUBMITTED)
