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
    projectId='mvp-project-273913',
    datasetId='michael',
    tableId='account_id_schema_480W')

output_spec = bigquery.TableReference(
    projectId='mvp-project-273913',
    datasetId='michael',
    tableId='yesyes')

dataflow_options = ['--project=mvp-project-273913','--job_name=gcp','--temp_location=gs://zz_michael/dataflow_s/tmp','--region=asia-east1']
dataflow_options.append('--staging_location=gs://zz_michael/dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = "dataflow"

table_schema = {
    'fields': [
        {'name': 'ACNO', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_1', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_2', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_3', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_4', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_5', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_6', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_7', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_8', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_9', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'FIELD_10', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ]
}



with beam.Pipeline(options=options) as p:
    quotes = p | 'read_source' >> beam.io.Read(beam.io.BigQuerySource(table_spec))

    quotes | 'write' >> beam.io.WriteToBigQuery(
        output_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
