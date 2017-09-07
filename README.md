# A Simple Job Manager for PBS (PJM)

[toc]

## Summary

PJM is a simple job manager for PBS (Portable Batch System) written by Python. It can be used as a framework for bioinformatics pipelines.

The main functions of PJM is inspirated by [SJM](https://github.com/StanfordBioinformatics/SJM "SJM"), but the jobs are defined like a function, which is distinct from SJM. PJM has beed tested on Torque. But I not sure whether it also work well on OpenPBS or PBS Professional. PJM could be ported to other scheduler in the future, such as SGE.

## Dependencies

Python 2.7
