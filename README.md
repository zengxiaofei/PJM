# A Simple Job Manager for PBS (PJM)

## Summary

PJM is a simple job manager for PBS (Portable Batch System) written by Python. The main functions of PJM is inspirated by [SJM](https://github.com/StanfordBioinformatics/SJM "SJM"). Similarlly, PJM can be used as a framework for bioinformatics analysis pipelines by automatically submitting jobs, monitoring job status and controlling the workflow according to dependencies of each job. However, jobs in PJM are defined like a "function", with all the settings including computing resources and job dependencies, which is distinct from SJM. 

PJM has beed tested on Torque. But it remains unknown Whether it also work well on OpenPBS or PBS Professional. PJM may be ported to or compatible with other schedulers in the future, such as SGE (Sun Grid Engine).

## Dependencies

Python 2.7

## Usage

Print help message:
```
$ pjm.py -h
usage: pjm.py [-h] [--sleep SLEEP] job_file

** A Simple Job Manager for PBS (PJM) Version 0.9 **
 . Author: Xiaofei Zeng
 . Email:  xiaofei_zeng@whu.edu.cn

positional arguments:
  job_file       input job file

optional arguments:
  -h, --help     show this help message and exit
  --sleep SLEEP  monitor interval time (seconds) [default: 60]
```
There is only one required argument `job_file`. 

To run a pipeline, just:
```
$ pjm.py a_pipeline.job
```
