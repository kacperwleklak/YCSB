cd ..
start cmd /k .\bin\ycsb.bat load creek -p creek.url=jdbc:postgresql://localhost:5433/postgres,jdbc:postgresql://localhost:5434/postgres ^
-p creek.user=postgres ^
-p creek.passwd=postgres ^
-p recordcount=10 ^
-p operationcount=10 ^
-p workload=site.ycsb.workloads.UuidCoreWorkload ^
-p workloadkeysfile=workloads/uuidkeys ^
-p insertstart=0 ^
-p readallfields=true ^
-p readproportion=0.0 ^
-p updateproportion=0.0 ^
-p scanproportion=0 ^
-p insertproportion=1 ^
-p requestdistribution=zipfian ^
-threads 10