import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions,StandardOptions
import random
from apache_beam.io.avroio import WriteToAvro

try:
    from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
    from avro.schema import parse as Parse  # avro library for python2

from fastavro import parse_schema
# import json
# import logging

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery

table_spec = bigquery.TableReference(
    projectId='mvp-project-273913',
    datasetId='rpm',
    tableId='newtable')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dataflow_options = ['--project=mvp-project-273913','--job_name=gcp','--temp_location=gs://zz_michael/dataflow_s/tmp','--region=asia-east1']
dataflow_options.append('--staging_location=gs://zz_michael/dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = "dataflow"

def cycle_dlqn(x):
    if x['FIELD_1'] > 0:
        x['NUM_OF_MTHS_PD_30'] = int(x['NUM_OF_MTHS_PD_30'])
        x['NUM_OF_MTHS_PD_30'] += 1
    else:
        x['NUM_OF_MTHS_PD_30'] = 0
    return x

class Join(beam.PTransform):
    """Composite Transform to implement left/right/inner/outer sql-like joins on
    two PCollections. Columns to join can be a single column or multiple columns"""

    def __init__(self, left_pcol_name, left_pcol, right_pcol_name, right_pcol, join_type, join_keys):
        """
        :param left_pcol_name (str): Name of the first PCollection (left side table in a sql join)
        :param left_pcol (Pcollection): first PCollection
        :param right_pcol_name: Name of the second PCollection (right side table in a sql join)
        :param right_pcol (Pcollection): second PCollection
        :param join_type (str): how to join the two PCollections, must be one of ['left','right','inner','outer']
        :param join_keys (dict): dictionary of two (k,v) pairs, where k is pcol name and
            value is list of column(s) you want to perform join
        """

        self.right_pcol_name = right_pcol_name
        self.left_pcol = left_pcol
        self.left_pcol_name = left_pcol_name
        self.right_pcol = right_pcol
        if not isinstance(join_keys, dict):
            raise TypeError("Column names to join on should be of type dict. Provided one is {}".format(type(join_keys)))
        if not join_keys:
            raise ValueError("Column names to join on is empty. Provide atleast one value")
        elif len(join_keys.keys()) != 2 or set([left_pcol_name, right_pcol_name]) - set(join_keys.keys()):
            raise ValueError("Column names to join should be a dictionary of two (k,v) pairs, where k is pcol name and "
                             "value is list of column(s) you want to perform join")
        else:
            self.join_keys = join_keys
        join_methods = {
            "left": UnnestLeftJoin,
            "right": UnnestRightJoin,
            "inner": UnnestInnerJoin,
            "outer": UnnestOuterJoin
        }
        try:
            self.join_method = join_methods[join_type]
        except KeyError:
            raise Exception("Provided join_type is '{}'. It should be one of {}".format(join_type, join_methods.keys()))

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, join_keys):
            return [data_dict[key] for key in join_keys], data_dict

        return ({pipeline_name: pcoll
                | 'Convert to ([join_keys], elem) for {}'.format(pipeline_name)
                    >> beam.Map(_format_as_common_key_tuple, self.join_keys[pipeline_name]) for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest CoGrouped' >> beam.ParDo(self.join_method(), self.left_pcol_name, self.right_pcol_name)
                )

class UnnestLeftJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the left pcol, and the matched records from the right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a left join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for source_dictionary in source_dictionaries:
            if join_dictionaries:
                # For each matching row from the join table, update the source row
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary
            else:
                # if there are no rows matching from the join table, yield the source row as it is
                yield source_dictionary


class UnnestRightJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the right pcol, and the matched records from the left pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a right join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for join_dictionary in join_dictionaries:
            if source_dictionaries:
                for source_dictionary in source_dictionaries:
                    join_dictionary.update(source_dictionary)
                    yield join_dictionary
            else:
                yield join_dictionary


class UnnestInnerJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records that have matching values in both the pcols"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an inner join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if not source_dictionaries or not join_dictionaries:
            pass
        else:
            for source_dictionary in source_dictionaries:
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary


class UnnestOuterJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records when there is a macth in either left pcol or right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an outer join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if source_dictionaries:
            if join_dictionaries:
                for source_dictionary in source_dictionaries:
                    for join_dictionary in join_dictionaries:
                        source_dictionary.update(join_dictionary)
                        yield source_dictionary
            else:
                for source_dictionary in source_dictionaries:
                    yield source_dictionary
        else:
            for join_dictionary in join_dictionaries:
                yield join_dictionary

table_schema = {
    'fields': [
        {"name": "ACNO", "type":'INTEGER', 'mode': 'NULLABLE'},
        {"name": "NUM_OF_MTHS_PD_30", "type":'INTEGER', 'mode': 'NULLABLE'},
        {"name": "FIELD_1", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_2", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_3", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_4", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_5", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_6", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_7", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_8", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_9", "type":'FLOAT', 'mode': 'NULLABLE'},
        {"name": "FIELD_10", "type":'FLOAT', 'mode': 'NULLABLE'}]
}

def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='mvp-project-273913', type=str, required=False, help='project')
    parser.add_argument('--job_name', default='rpm', type=str)
    parser.add_argument('--worker_node', default='n1-standard-4')
    parser.add_argument('--temp_location', default='gs://zz_michael/dataflow_s/tmp')
    parser.add_argument('--location', default='gcs')
    parser.add_argument('--region', default='asia-east1')
    parser.add_argument('--staging_location', default='gs://zz_michael/dataflow_s/stage')
    parser.add_argument('--output', required=False,
                        default='gs://zz_michael/dataflow_s/RPM/output/account_id_schema_output.avro',
                        help='Output file to write results to.')
    parser.add_argument('--input', default='gs://zz_michael/dataflow_s/RPM/Curr_account.avro',
                        help='input file to write results to.')
    parser.add_argument('--input2', default='gs://zz_michael/dataflow_s/RPM/Prev_account.avro',
                        help='input file to write results to.')                   
    # Parse arguments from the command line.
    # known_args, pipeline_args = parser.parse_known_args(argv)
    args = parser.parse_args()

    dataflow_options = ['--project=%s' % (args.project), '--job_name=%s' % (args.job_name),
                        '--temp_location=%s' % (args.temp_location), '--worker_machine_type=%s' % (args.worker_node),
                        '--region=%s' % (args.region)]

    dataflow_options.append('--staging_location=%s' % (args.staging_location))
    options = PipelineOptions(dataflow_options)
    gcloud_options = options.view_as(GoogleCloudOptions)

    options.view_as(StandardOptions).runner = "dataflow"


    SCHEMA = {"namespace": "example.avro",
              "type": "record",
              "name": "User",
              "fields": [
                  {"name": "ACNO", "type": ["null", {"logicalType": "char", "type": "string", "maxLength": 20}]},
                  {"name": "NUM_OF_MTHS_PD_30", "type": ["null",'int','string']},
                  {"name": "FIELD_1", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_2", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_3", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_4", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_5", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_6", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_7", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_8", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_9", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_10", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]}
              ]
              }

    with beam.Pipeline(options=options) as p:
        left_pcol_name = 'p1'
        Curr_Month = p | 'read_Curr_Month' >> beam.io.ReadFromAvro(args.input) 
        Prev_Month = p | 'read_Prev_Month' >> beam.io.ReadFromAvro(args.input2)
        p1 = Curr_Month | 'select fields from Curr_Month' >> beam.Filter(lambda x: int(x['NUM_OF_MTHS_PD_30']) >= 0) 
        p2 = Prev_Month | 'select fields2 from Prev_Month' >> beam.Filter(lambda x: int(x['NUM_OF_MTHS_PD_30']) >= 0) 
        right_pcol_name = 'p2'

        join_keys = {
            left_pcol_name: [
                'ACNO'
                # 't1_col_B'
            ],
            right_pcol_name: [
                'ACNO'
                # 't2_col_B'
            ]}

        joinkey_dict = {left_pcol_name: p1, right_pcol_name: p2}
        joined_data = joinkey_dict | 'left join' >> Join(left_pcol_name=left_pcol_name, left_pcol=p1,
                                                                   right_pcol_name=right_pcol_name, right_pcol=p2,
                                                                   join_type='left', join_keys=join_keys) 
        derived_result = joined_data | 'Transform (add 1 to fileld)' >> beam.Map(cycle_dlqn)                                       
        print(type(joined_data))
        compressIdc = True
        use_fastavro = True


        compressIdc = True
        use_fastavro = True

        if args.location =="bigquery":
            derived_result | beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED) 
        else:
            derived_result | 'Write out Stage' >> WriteToAvro(
                args.output,
                parse_schema(SCHEMA),
                use_fastavro=use_fastavro,
                file_name_suffix='.avro',
                codec=('deflate' if compressIdc else 'null'),
            )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()