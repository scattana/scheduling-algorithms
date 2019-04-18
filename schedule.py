# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 14:41:04 2019

@author: scatt

CSE-34341 Operating Systems
Assignment 3 - Spring 2019
Seth Cattanach (scattana)
Due: Wednesday, 27-Mar 2019
"""

import csv
from operator import itemgetter

# utility function to swap two items in a list
def shift(full, i, j):
    temp = full[j]
    icount = 0
    while icount < (j-i):
        full[j-icount] = full[j-icount-1]
        icount += 1
    full[i] = temp
    return full

# utility function to perform linear search and return first job that has arrived (job[5]=='y')
def lin_search(full, i):
    for index in range(len(full)):
        if full[index][6] == 'y':
            continue
        if full[index][5] == 'y':
            return index
    return -1         # if no jobs have yet arrived, return null sentinel (-1)

# utility function to parse the +x-y burst notation and return a list with the format [x, -y]
def parse_bursts(burst):
    ret = []
    for i in range(len(burst)):
        if burst[i].isdigit():
            continue
        if burst[i] == '+':
            temp = i
            while burst[temp+1].isdigit():
                temp += 1
                if temp+1 == len(burst):
                    break
            ret.append(int(burst[i+1:temp+1]))
        elif burst[i] == '-':
            temp = i
            while burst[temp+1].isdigit():
                temp += 1
                if temp+1 == len(burst):
                    break
            ret.append(int(burst[i+1:temp+1])*-1)
    return ret
    

def first_come_first_served(jobs, io_sum=0, sjf=False, fewest_bursts=False):
    if sjf:
        print('\nALGORITHM: SHORTEST JOB FIRST\n---------------------------')
    elif fewest_bursts:
        print('\nALGORITHM: FEWEST BURSTS\n------------------------')
    else:
        print('\nALGORITHM: FIRST COME FIRST SERVED\n--------------------------------')
    num_jobs = len(jobs)
    i_job = 0
    t = 0           # generic time units to simulate running time
    tt_time = 0     # turnaround time (keep a running sum, then divide at the end)
    while(True):
        for job in jobs:
            if int(job[1]) <= t and job[5] == 'n':
                job[5] = 'y'
        if jobs[i_job][6] == 'y':
            i_job += 1      # job has already completed
        if jobs[i_job][5] == 'n':
            j = lin_search(jobs, i_job)
            if j == -1:
                t += 1
                continue
            jobs = shift(jobs, i_job, j)    # only process jobs that have arrived

        if not sjf:
            temp = parse_bursts(jobs[i_job][2])
            io_sum += sum((-1*item) for item in temp if item < 0)
        t = execute_nonpreemptive(t, jobs[i_job])   # execute until completion and update new time
        tt_time += (t-int(jobs[i_job][1]))
#        print('t='+str(t)+'\t\t'+jobs[i_job][0]+' completed, added to tt_time: '+str(t-int(jobs[i_job][1])))
        jobs[i_job][6] = 'y'
        i_job += 1      # increment i_job to indicate completion
        if i_job == num_jobs:
            break
    
    # after WHILE loop, calculate and print metrics:
    print('\nAverage turnaround time: '+str(tt_time/num_jobs))
    print('Total time to completion: '+str(t))
    print('Balance (% CPU occupied): '+str(((t-io_sum)/t)*100))

def shortest_job_first(jobs):
    # sort by total execution time required, then execute in order (non-preemptive)
    io_sum = 0
    for job in jobs:
        temp = parse_bursts(job[2])
        io_sum += sum(item for item in temp if item < 0)
        job[2] = sum(map(int, filter(None, job[2].replace('+','-').split('-'))))
    sorted_jobs = sorted(jobs, key=itemgetter(2))
    for item in sorted_jobs:
        item[2] = str(item[2])
    first_come_first_served(sorted_jobs, io_sum*-1, sjf=True)



def execute_nonpreemptive(s_time, job):
#    print('Executing job '+job[0]+' at t='+str(s_time)+' time units')
    time_req = sum(map(int, filter(None, job[2].replace('+','-').split('-'))))
    e_time = s_time + time_req      # simulate time passing
#    print('Completed job '+job[0]+' at t='+str(e_time)+ ' time units')
    return e_time

def shortest_time_to_completion(jobs):
    print('\nALGORITHM: SHORTEST TIME TO COMPLETION\n---------------------------------------')
    num_io = 0
    for job in jobs:                                    # parse bursts into easier-to-use format
        job[2] = parse_bursts(job[2])
    completed_count = 0
    t = 0
    tt_time = 0
    num_jobs = len(jobs)
#    jobs = sorted(jobs, key=lambda x: x[2][0])         # sort each job by its first CPU burst
    while(True):
        for job in jobs:                                # mark any new jobs as 'arrived'
            if int(job[1]) <= t and job[5] == 'n':
                job[5] = 'y'
        jobs = sorted(jobs, key=lambda x: x[2][0])      # sort each job by its first CPU burst
        cpu_flag = False
        for job in jobs:
            if job[5] == 'y' and job[6] == 'n' and job[2][0] > 0:
                cpu_flag = True
        if not cpu_flag:
            for job in jobs:
                if job[2][0] < 0:
                    job[2][0] += 1                      # execute IO burst in background                    
            t += 1                                      # edge case where all jobs are currently IO-bound
            num_io += 1
        for job in jobs:
            if job[5] == 'y' and job[6] == 'n' and job[2][0] > 0:
                for i in range(job[2][0]):
                    job[2][0] -= 1
                    for other_job in jobs:
                        if other_job[2][0] < 0:
                            other_job[2][0] += 1
                            if other_job[2][0] == 0:
                                other_job[2].pop(0)
                    t += 1
                break                                   # so that only one job can occupy CPU at a given time
        for index, job in enumerate(jobs):
            if job[2][0] == 0:
                job[2].pop(0)                        # that burst has completed, so pop it from burst list
                if not job[2]:
                    job[6] == 'y'                       # job has completed when it has no more bursts
                    tt_time += (t-int(job[1]))          # increment turnaround time for completed job
 #                   print('t='+str(t)+'\t\t'+job[0]+' complete, added to tt_time: '+str(t-int(job[1])))
                    completed_count += 1
                    jobs.pop(index)
        if completed_count == num_jobs:
            print('\nAverage turnaround time: '+str(tt_time/num_jobs))
            print('Total time to completion: '+str(t))
            print('Balance (% CPU occupied): '+str(((t-num_io)/t)*100))
            break
        
def round_robin(jobs, q=10, priority=False):
    if priority:
        print('\nALGORITHM: PRIORITY (q='+str(q)+')\n----------------------------------')
    else:
        print('\nALGORITHM: ROUND ROBIN (q='+str(q)+')\n------------------------------')
    num_wait_on_io = 0
    for job in jobs:                                    # parse bursts into easier-to-use format
        job[2] = parse_bursts(job[2])
    completed_count = 0
    t = 0
    tt_time = 0
    num_jobs = len(jobs)
    temp = 0                                            # track progress in current quanta
    while(True):
        if priority:
            jobs = sorted(jobs, key=lambda x: x[3])
        cpu_flag = False                                # track whether there are any CPU-bound jobs waiting
        for job in jobs:                                # mark new jobs as arrived
            if int(job[1]) <= t and job[5] == 'n':
                job[5] = 'y'
        for i,job in enumerate(jobs):
            for item in jobs:
                if item[2][0] > 0:
                    cpu_flag = True
            if not cpu_flag:
                for bg in jobs:
                    num_wait_on_io += 1
                    bg[2][0] += 1                       # execute all IO-bound jobs in bg
                    t += 1
            if job[2][0] > 0 and job[5] == 'y' and job[6] == 'n':
#                print('t='+str(t)+'\tfound job: '+job[0]+'\ttemp='+str(temp),jobs)
                # execute job for one quanta
                t += 1
                temp += 1
                job[2][0] -= 1
                for bg in jobs:
                    if bg[2][0] < 0:
                        bg[2][0] += 1                   # execute all IO-bound jobs in background
                if temp == q or job[2][0] <= 0:
                    temp = 0
                    jobs.append(job)
                    jobs.pop(i)
                break
        for job in jobs:
            if job[2][0] == 0:
                job[2].pop(0)
        for i,job in enumerate(jobs):
            if not job[2]:
                completed_count += 1
                tt_time += (t-int(job[1]))
                jobs.pop(i)
        if completed_count == num_jobs:
            print('\nAverage turnaround time: '+str(tt_time/num_jobs))
            print('Total time to completion: '+str(t))
            print('Balance (% CPU occupied): '+str(((t-num_wait_on_io)/t)*100))
            break

def priority(jobs):
    round_robin(jobs, priority=True)            # round-robin was designed with parameters to use priority alg


def fewest_bursts(jobs):
    # loop through jobs and add additional parameter to track number of bursts for a given job
    for job in jobs:
        job.append(len(parse_bursts(job[2])))                   # parse, split, count bursts - then append
    sorted_jobs = sorted(jobs, key=itemgetter(7))               # sort by the sum found in previous step
    
    first_come_first_served(sorted_jobs, fewest_bursts=True)    # use modified FCFS alg w/special param
               

def main():
    jobs = []
    with open("jobs.txt") as fd:
        rd = csv.reader(fd, delimiter='\t')
        for row in rd:
            row += 'n'  # add a binary field to track whether or not the job has arrived
            row += 'n'  # add a binary field to track whether or not the job has completed
            jobs.append(row)
    
    # UNCOMMENT THE ALGORITHM YOU WISH TO TEST FROM THE OPTIONS BELOW:
    # DO NOT UNCOMMENT MULTIPLE ALGORITHMS AT ONCE
    first_come_first_served(jobs)              # Non-preemptive
#    shortest_job_first(jobs)                   # Non-preemptive
#    shortest_time_to_completion(jobs)          # Preemptive
#    round_robin(jobs, q=4)                     # Preemptive (specify q as opt param)
#    priority(jobs)                             # Preemptive
#    fewest_bursts(jobs)                         # my own algorithm; non-preemptive
            


if __name__ == "__main__":
    main()


