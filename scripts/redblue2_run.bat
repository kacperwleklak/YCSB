cd ..
start cmd /k .\bin\ycsb run redblue -p redblue.url=jdbc:postgresql://localhost:5433/postgres,jdbc:postgresql://localhost:5434/postgres ^
    -p redblue.user=postgres ^
    -p redblue.passwd=postgres ^
    -p redblue.red.failure-prob=1 ^
    -p recordcount=11000 ^
    -p operationcount=11000 ^
    -p workload=site.ycsb.workloads.UuidCoreWorkload ^
    -p workloadkeysfile=workloads/uuidkeys ^
    -p insertstart=1000 ^
    -p readallfields=true ^
    -p readproportion=0 ^
    -p updateproportion=1 ^
    -p scanproportion=0 ^
    -p insertproportion=0 ^
    -p requestdistribution=zipfian ^
    -threads 50