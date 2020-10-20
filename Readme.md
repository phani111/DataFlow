# this is for setup the google cloud env

## [one-off] create in console

1. create service account
appdevops-sa@mvp-project-273913.gserviceaccount.com

2. create a VM instance (kubectl-2 in dick's project mvp-project-273913)

3. ssh into VM and build a virtual env with the following commands

```linux
pip3 install --upgrade virtualenv  --user
gcloud auth list
python3 -m virtualenv env
source env/bin/activate
pip3 install --quiet apache-beam[gcp]
gsutil mb gs://mvp-project-273913
gsutil cp gs://zz_michael/dataflow_s/dataflow-mvp.py .
source env/bin/activate
```

4. run dataflow-mvp.py
```linux
gsutil cp gs://zz_michael/dataflow_s/dataflow-mvp.py .
python dataflow-mvp.py --job_name dl-960w-n16 --worker_node n1-standard-16
```
worker_node is worker machine type, for example n1-standard-8,n1-standard-4
input is the source file



Record   | output | vm              | Elapsed time 
---------|--------|-----------------|------------- 
9,600,000| gcs    | n1-standard-16  |7 min 22 sec  
9,600,000| gcs    | n1-standard-8   |9 min 10 sec  
9,600,000| gcs    | n1-standard-4   |13 min 12 sec 
9,600,000| gcs    | n1-standard-1   |15 min 28 sec 
