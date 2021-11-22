- Need to be root to generate sacct reports for all accounts
- Dump all accounts
```
$ sudo sacctmgr dump clustername
$ sudo grep Fairshare=XXXX$ clustername.cfg | awk '{print $3}' | cut -f1 -d: | sed -e "s/'//g" > projects.txt
```
- Command line: 
```
sacct -P -o "JobID%20,User,Partition,State,ExitCode,TotalCPU,MaxVMSize,MaxDiskRead,MaxDiskWrite,Submit,Start,Elapsed" -T -S2021-08-01T00:00 -E2021-09-01T00:00  -A $(cat projects.txt | xargs | sed -e 's/\ /,/g') > sacct_202108.txt
```
