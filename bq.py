from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.internal.clients import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

table_spec = bigquery.TableReference(
    projectId='query-11',
    datasetId='rpm',
    tableId='account_id_schema_new')

dataflow_options = ['--project=query-11','--job_name=amaz','--temp_location=gs://dataflow_s/tmp','--region=us-central1']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = "dataflow"

with beam.Pipeline(options=options) as p:
    max_temperatures = (
        p
        | 'ReadTable' >> beam.io.Read(beam.io.BigQuerySource(table_spec)))
