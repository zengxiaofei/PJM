#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A Simple Job Manager for PBS

Author: Xiaofei Zeng
Email: xiaofei_zeng@whu.edu.cn
Version: V0.9
"""

from __future__ import print_function
import sys
import os
import re
import ConfigParser
import argparse
import time
import collections
import socket


class Job(object):
    """creat a Job object"""
    def __init__(self, job_name):
        self.name = job_name
        # default settings
        self.depend = set()
        self.nodes = default_nodes
        self.ppn = default_ppn
        self.queue = default_queue
        self.cmd = ''
        self.dir = workdir + job_name
        self.status = 'waiting'
        self.jobid = ''
        self.stringency = default_stringency
        self.realtime = 0
        self.cputime = 0
    
    def generate_pbs(self, cfg_header):
        os.system('mkdir {0}'.format(self.dir))
        with open('{0}/{1}.pbs'.format(self.dir, self.name), 'w') as fpbs:
            fpbs.write('#PBS -N {0}\n'.format(self.name))
            fpbs.write('#PBS -l nodes={0}:ppn={1}\n'.format(self.nodes, self.ppn))
            fpbs.write('#PBS -q {0}\n'.format(self.queue))
            fpbs.write('#PBS -S /bin/bash\n')
            if self.stringency == 'high':
                fpbs.write('set -o errexit\n')
            elif self.stringency == 'low':
                pass
            fpbs.write('cd {0}\n'.format(self.dir))
            fpbs.write('echo Job started at `date` @`hostname`\n')
            fpbs.write(cfg_header)
            fpbs.write(self.cmd)
            fpbs.write('echo Job completed at `date`\n')

    def qsub(self):
        jobid = os.popen('cd {0} && qsub {1}.pbs'.format(
                self.dir, self.name)).read().rstrip()
        self.jobid = jobid
        self.status = 'running'
        with open(log_file, 'a') as fqsub:
            fqsub.write('{0}: job {1} submitted ({2})\n'.format(
                    time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),
                    self.name, self.jobid))
    
    def status_monitor(self):
        # running
        if self.jobid:
            is_running = os.popen(
                   'qstat | awk "NR>2 {print $1}" | grep "%s"' % self.jobid).read().rstrip()
        else:
            is_running = False
        
        if is_running:
            self.status = 'running'
        # done with no errors
        elif self.status == 'running':
            done_no_errors = os.popen(
                    'tail -1 {0}/{1}.o{2} 2>/dev/null | grep -P "^Job completed at "'.format(
                            self.dir, self.name, self.jobid.split('.')[0])).read()
            if done_no_errors:
                self.status = 'done'
        # aborted with errors
            else:
                self.status = 'error'


def analyse_dependency():
    #TODO analyse dependency for PBS, Python
    pass


def get_arg(arg_name, args_string):
    match_arg =  re.match(
            r'[\w\W]*'+arg_name+r' *= *([\w,.]+)', 
            args_string)
    if match_arg:
        return match_arg.group(1)


def job_parser(job_file):
    job_list = []
    config_start = False
    cmd = ''
    is_end = True
    cfg_header = ''
    with open(job_file, 'r') as f:
        for line in f:
            # config parser
            if line.lstrip().startswith('##'):
                config_start = not config_start
                if not config_start:
                    cfg_header += line
            if config_start and line.strip():
                cfg_header += line
            # job parser
            match_line = re.match(r' *([\w.]+) *\(([\w\W]*)\){', line)
            # job start
            if match_line:
                is_end = False
                job_name = match_line.group(1)
                job = Job(job_name)
                job_list.append(job)
                args_string = match_line.group(2)
                depend = get_arg('depend', args_string)
                nodes = get_arg('nodes', args_string)
                ppn = get_arg('ppn', args_string)
                queue = get_arg('queue', args_string)
                dir = get_arg('dir', args_string)
                status = get_arg('status', args_string)
                stringency = get_arg('stringency', args_string)
                # TODO: need a robust func to validate args
                if depend:
                    job.depend = set(depend.split(','))
                if nodes:
                    job.nodes = nodes
                if ppn:
                    job.ppn = ppn
                if queue:
                    job.queue = queue
                if dir:
                    job.dir = workdir + dir
                if status:
                    if status in ('error', 'running'):
                        os.system('rm -r {0}'.format(job.dir))
                        job.status = 'waiting'
                    else:
                        job.status = status
                if stringency:
                    job.stringency = stringency
            # job end
            elif re.match(r' *} *END', line) and not config_start:
                is_end = True
            # job commands
            elif not (is_end or config_start):
                job.cmd += line
    return (job_list, cfg_header)


def wait_finish(job_list, cfg_header, sleeptime):
    """if one job error, wait for other jobs to finish"""
    while True:
        run_job = 0
        for job in job_list:
            job.status_monitor()
            if job.status == 'running':
                run_job += 1
        if not run_job:
            break
        log(job_list, cfg_header)
        time.sleep(sleeptime)



def log(job_list, cfg_header):
    """generate .status .log .rsc"""
    def calculate_time(time_str):
        ts = time_str.split(':')
        return int(ts[0])*60*60 + int(ts[1])*60 + int(ts[2])

    def status_log(job_list, cfg_header):
        with open(status_file, 'w') as fstatus:
            fstatus.write(cfg_header)
            for job in job_list:
                depend_arg = 'depend={0};'.format(','.join(job.depend)) if job.depend else ''
                fstatus.write('%s(%s%s){\n%s}END\n\n' % (
                        job.name,
                        depend_arg,
                        ';'.join(('nodes='+job.nodes,
                                  'ppn='+job.ppn,
                                  'queue='+job.queue,
                                  'dir='+os.path.basename(job.dir),
                                  'stringency='+job.stringency,
                                  'status='+job.status)),
                        job.cmd))

    def event_log():
        logstr = ''
        for status in ('running', 'waiting', 'done', 'error'):
            if not status_dict[status]:
                pass
            elif logstr:
                logstr += '; {0} {1}'.format(','.join(status_dict[status]), status)
            else:
                logstr += '{0} {1}'.format(','.join(status_dict[status]), status)
        
        last_line = ''
        if os.path.exists(log_file):
            with open(log_file) as f:
                for line in f:
                    last_line = line
        else:
            # record some info at first
            with open(log_file, 'w') as fevent:
                fevent.write('PJM running @{0}\n'.format(socket.gethostname()))
        
        with open(log_file, 'a') as fevent:
            if last_line and logstr != last_line.split(' ', 2)[2].strip():
                fevent.write('{0}: {1}\n'.format(
                        time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), logstr))
                status_log(job_list, cfg_header)
    
    def resource_log():
        # TODO statistics of realtime and cputime, not compatible with SGE
        with open(resource_file, 'a') as frsc:
            frsc.write('################# {0} #################\n'.format(
                    time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
            frsc.write('Job_Name\tCPU_Time(s)\tReal_Time(s)\tAvg_CPU\n')
            for job in job_list:
                if job.status == 'running':
                    with os.popen('qstat -f {0}'.format(job.jobid)) as f:
                        for line in f:
                            match_cpu = re.match(r'.*resources_used.cput = ([\d:]+)\n', line)
                            match_real = re.match(r'.*resources_used.walltime = ([\d:]+)\n', line)
                            if match_cpu:
                                cputime = match_cpu.group(1)
                                job.cputime = calculate_time(cputime)
                            if match_real:
                                realtime = match_real.group(1)
                                job.realtime = calculate_time(realtime)
                avg_cpu = 'NA' if job.realtime == 0 else float(job.cputime)/job.realtime
                frsc.write('{0}\t{1}\t{2}\t{3:.3f}\n'.format(
                        job.name, job.cputime, job.realtime, avg_cpu))
        
    status_dict = collections.defaultdict(list)
    for job in job_list:
        job.status_monitor()
        status_dict[job.status].append(job.name)
    
    event_log()
    resource_log()


def job_manager(job_list, cfg_header, sleeptime):
    done_set = set()
    try:
        while True:
            log(job_list, cfg_header)
            for job in job_list:
                job.status_monitor()
                # if no pre-jobs
                if job.status == 'waiting' and not job.depend:
                    job.generate_pbs(cfg_header)
                    job.qsub()
                    job.status_monitor()
                # if job is done
                elif job.status == 'done':
                    for otherjob in job_list:
                        if job.name in otherjob.depend:
                            otherjob.depend.remove(job.name)
                    done_set.add(job)
                elif job.status == 'error':
                    raise RuntimeError
            if done_set == set(job_list):
                log(job_list, cfg_header)
                break
            time.sleep(sleeptime)
    # if job is aborted with errors
    except:
        # wait for rest job finished
        wait_finish(job_list, cfg_header, sleeptime)
        log(job_list, cfg_header)


def daemonize(stdin='/dev/null',stdout= '/dev/null', stderr= 'dev/null'):
    #Perform first fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)  #first parent out
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" %(e.errno, e.strerror))
        sys.exit(1)

    os.chdir("/")
    os.umask(022)
    os.setsid()
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) #second parent out
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s]n" %(e.errno,e.strerror))
        sys.exit(1)

    for f in sys.stdout, sys.stderr:
        f.flush()
    si = open(stdin, 'r')
    so = open(stdout,'a+')
    se = open(stderr,'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def main(): 
    # parse job file
    job_list, cfg_header = job_parser(job_file)
    job_manager(job_list, cfg_header, sleeptime)


if __name__ == '__main__':
    ## configparse
    config = ConfigParser.ConfigParser()
    config.read(os.path.split(os.path.realpath(__file__))[0]+'/pjm.cfg')
    default_nodes = config.get('Default', 'nodes')
    default_ppn = config.get('Default', 'ppn')
    default_queue = config.get('Default', 'queue')
    default_stringency = config.get('Default', 'stringency')
    queue_list = config.get('PBS', 'QueueList').split(',')
    
    ## argparse
    parser = argparse.ArgumentParser(
            formatter_class = argparse.RawDescriptionHelpFormatter,
            description = """** A Simple Job Manager for PBS (PJM) Version 0.9 **
 . Author: Xiaofei Zeng
 . Email: xiaofei_zeng@whu.edu.cn""")
    parser.add_argument('job_file', help='input job file')
    parser.add_argument('--sleep', default=60, type=int, 
                        help='monitor interval time (seconds) [default: %(default)i]')
    
    analyse_dependency() 
    
    args = parser.parse_args() 
    sleeptime = args.sleep
    job_file = os.path.abspath(args.job_file)
    job_filename = os.path.basename(args.job_file)
    workdir = os.getcwd() + '/'
    log_file = workdir + job_filename + '.log'
    status_file = workdir + job_filename + '.status'
    resource_file = workdir + job_filename + '.rsc'
    daemon_out = workdir + job_filename + '.out'
    
    daemonize('/dev/null', daemon_out, daemon_out)
    main()
