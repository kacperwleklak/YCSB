cd ..
start cmd /k .\bin\ycsb.bat load redblue -p redblue.url=jdbc:postgresql://localhost:5433/postgres,jdbc:postgresql://localhost:5434/postgres,postgresql://localhost:5435/postgres ^
-p redblue.user=postgres ^
-p redblue.passwd=postgres ^
-p redblue.red.failure-prob=1 ^
-p recordcount=9 ^
-p operationcount=9 ^
-p workload=site.ycsb.workloads.UuidCoreWorkload ^
-p workloadkeysfile=workloads/uuidkeys ^
-p insertstart=0 ^
-p readallfields=true ^
-p readproportion=0.5 ^
-p updateproportion=0.5 ^
-p scanproportion=0 ^
-p insertproportion=0 ^
-p requestdistribution=zipfian ^
-threads 10