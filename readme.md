# this is for setup the google cloud env

## [one-off] create in console

1. create service account
appdevops-sa@mvp-project-273913.gserviceaccount.com

2. download the keys as gcp.json
3. run

```bash
pip install 'apache-beam[gcp]'
pip install six==1.14.0
pip install --upgrade google-cloud-storage

gcloud components update beta
```

```bash
sudo pip3 install 'apache-beam[gcp]' --quiet
sudo pip3 install six==1.14.0
sudo pip3 install --upgrade google-cloud-storage

# AWS only
cd /opt/
sudo wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-280.0.0-linux-x86_64.tar.gz
sudo tar -xf google-cloud-sdk-280.0.0-linux-x86_64.tar.gz
sudo sudo su
export PATH=$PATH:/opt/google-cloud-sdk/bin
gcloud components update beta

# GCP only
sudo apt-get install google-cloud-sdk
```

from google.cloud import storage
storage_client = storage.Client()
buckets = list(storage_client.list_buckets())
print(buckets)




## [SSR](https://qhh.me/2018/04/14/%E9%85%8D%E7%BD%AE-gcloud-%E4%BD%BF%E7%94%A8-Shadowsocks-HTTP-%E4%BB%A3%E7%90%86/)

## setup for cli

```bash
GCP_Project_ID=mvp-project-273913
build_id=rbwm-sbox
zone=asia-east1-a
region=asia-east1

gcloud config configurations create ${build_id}
gcloud config set core/project ${GCP_Project_ID} --configuration ${build_id}
gcloud config set compute/zone ${zone} --configuration ${build_id}
gcloud config set compute/region ${region} --configuration ${build_id}
gcloud config set pass_credentials_to_gsutil true --configuration ${build_id}

#gcloud config set pass_credentials_to_gsutil false

gcloud auth activate-service-account --key-file ~/gcp.json --configuration ${build_id}
gcloud iam roles describe roles/dataflow.serviceAgent

gcloud config set proxy/type http --configuration ${build_id}
gcloud config set proxy/address 127.0.0.1 --configuration ${build_id}
gcloud config set proxy/port 1087 --configuration ${build_id}


gcloud config set proxy/type http
gcloud config set proxy/address 18.139.219.80 --configuration ${build_id}
gcloud config set proxy/port 3128 --configuration ${build_id}
gcloud config set pass_credentials_to_gsutil true




gcloud config get-value proxy/type
gcloud config get-value proxy/address
gcloud config get-value proxy/port


gcloud config unset proxy/type
gcloud config unset proxy/address
gcloud config unset proxy/port


```

## DirectRunner

```bash
gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt /tmp/kinglear.txt

python -m apache_beam.examples.wordcount --input /tmp/kinglear.txt --output /tmp/beam.txt

python examples/wordcount.py --input /tmp/kinglear.txt --output /tmp/beam.txt --runner DirectRunner
python examples/wordcount.py --input /tmp/kinglear.txt --output /tmp/beam.txt --runner DataflowRunner

python examples/wordcount.py --input gs://dataflow-samples/shakespeare/kinglear.txt --output /tmp/beam.txt --runner DirectRunner
python examples/wordcount.py --project $PROJECT --temp_location gs://$BUCKET/tmp/  --input gs://dataflow-samples/shakespeare/kinglear.txt --output /tmp/beam.txt --runner DirectRunner
```

## DataflowRunner

gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt gs://$BUCKET/kinglear.txt

gsutil cp gs://dy-awesome-bucket/kinglear.txt /tmp/kinglear.txt
```bash
gsutil mb -l asia-east1 gs://dy-awesome-bucket/

PROJECT=mvp-project-273913
BUCKET=dy-awesome-bucket
REGION=asia-east1

python3 -m apache_beam.examples.wordcount \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://$BUCKET/wordcount/outputs \
    --runner DataflowRunner \
    --project $PROJECT \
    --temp_location gs://$BUCKET/tmp/

wget https://raw.githubusercontent.com/apache/beam/master/sdks/python/apache_beam/examples/wordcount.py
export GOOGLE_APPLICATION_CREDENTIALS=~/gcp.json
python3 wordcount.py  \
    --project $PROJECT \
    --input gs://$BUCKET/kinglear.txt \
    --output gs://$BUCKET/wordcount/outputs \
    --runner DataflowRunner \
    --temp_location gs://$BUCKET/tmp/ \

    
    python -m apache_beam.examples.wordcount \
      --region $REGION \
      --input gs://dataflow-samples/shakespeare/kinglear.txt \
      --output gs://$BUCKET/wordcount/outputs \
      --runner DataflowRunner \
      --project $PROJECT \
      --temp_location gs://$BUCKET/tmp/

export GOOGLE_APPLICATION_CREDENTIALS=/Users/dickye/gcp.json
python examples/wordcount.py  \
    --project $PROJECT \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://$BUCKET/wordcount/outputs \
    --runner DataflowRunner \
    --temp_location gs://$BUCKET/tmp/ \
    --service-accountn-email=appdevops-sa@mvp-project-273913.iam.gserviceaccount.com \


python examples/join/two_gcs_inputs.py  \
    --project $PROJECT \
    --runner DataflowRunner \
    --temp_location gs://$BUCKET/tmp/ \
    --service-accountn-email=appdevops-sa@mvp-project-273913.iam.gserviceaccount.com \


git clone https://github.com/yeahydq/dataflow_demo.git
cd dataflow_demo
export PYTHONPATH=`pwd`
python3 demos/join/demo_join2.py \
--setup_file ./setup.py \
--records 1000 \
--output /tmp/dataflow/demo/output \
--runner DirectRunner

export GOOGLE_APPLICATION_CREDENTIALS=~/gcp.json
python3 demos/join/demo_join2.py  \
--setup_file ./setup.py \
--project mvp-project-273913 --records 1000 \
--output gs://dy-awesome-bucket/demo/outputs \
--temp_location gs://dy-awesome-bucket/tmp/ \
--runner DataflowRunner



Record | mac | vm | dataflow
-------|-----|----|---------
1000 | 0.11 | |
