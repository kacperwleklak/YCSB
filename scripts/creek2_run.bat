cd ..
start cmd /k .\bin\ycsb.bat run creek -p creek.url=jdbc:postgresql://localhost:5433/postgres,jdbc:postgresql://localhost:5434/postgres ^
-p creek.user=postgres ^
-p creek.passwd=postgres ^
-p recordcount=1000 ^
-p operationcount=1000 ^
-p workload=site.ycsb.workloads.UuidCoreWorkload ^
-p workloadkeysfile=workloads/uuidkeys ^
-p insertstart=10 ^
-p readallfields=true ^
-p readproportion=0 ^
-p updateproportion=1 ^
-p scanproportion=0 ^
-p insertproportion=0 ^
-p requestdistribution=zipfian ^
-threads 5