import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from join import Join
import random
from apache_beam.io.avroio import WriteToAvro

try:
    from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
    from avro.schema import parse as Parse  # avro library for python2

from fastavro import parse_schema
import json
import logging

def add_one(x):
    if int(x['NUM_OF_MTHS_PD_30']) >= 1:
        x['NUM_OF_MTHS_PD_30'] = int(x['NUM_OF_MTHS_PD_30'])
        x['NUM_OF_MTHS_PD_30'] += 1
    else:
        x['NUM_OF_MTHS_PD_30'] = 0
    return x
def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    # parser.add_argument('--project', type=str, required=False, help='project')
    parser.add_argument(
        '--records',
        dest='records',
        type=int,
        # default='gs://dataflow-samples/shakespeare/kinglear.txt',
        default='10',  # gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt
        help='Number of records to be generate')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='./',
        help='Output file to write results to.')
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Store the CLI arguments to variables
    # project_id = known_args.project

    # Setup the dataflow pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = True
    # google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # google_cloud_options.project = project_id

    save_main_session = True
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(pipeline_args)

    with beam.Pipeline() as pipeline:
        total = pipeline | 'Create plant counts' >> beam.Create([
          ('1', 3),
          ('1', 2),
          ('2', 1),
          ('3', 4),
          ('4', 5),
          ('4', 3),
      ])| 'Sum' >> beam.Distinct() | beam.Map(print)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()