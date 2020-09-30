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
# from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.internal.clients import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




dataflow_options = ['--project=query-11','--job_name=amaz','--temp_location=gs://dataflow_s/tmp','--region=us-central1']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = "dataflow"


input_filename = "gs://dataflow_s/RPM/account_id_schema_new.avro"
output_filename = "gs://dataflow_s/RPM/account_id_schema_output.avro"


def printfn(elem):
    print(elem)

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


def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='query-11',type=str, required=False, help='project')
    parser.add_argument('--job_name', default='rpm', type=str)
    parser.add_argument('--temp_location', default='gs://dataflow_s/tmp')
    parser.add_argument('--region', default='us-central1')
    parser.add_argument('--staging_location', default='gs://dataflow_s/stage')
    parser.add_argument('--runner', default='DataflowRunner')
    parser.add_argument(
        '--records',
        dest='records',
        type=int,
        # default='gs://dataflow-samples/shakespeare/kinglear.txt',
        default='10',  # gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt
        help='Number of records to be generate')
    parser.add_argument('--output',required=False,default='gs://dataflow_s/RPM/account_id_schema_output.avro',help='Output file to write results to.')
    parser.add_argument('--input',default='gs://dataflow_s/RPM/account_id_schema_new.avro',help='input file to write results to.')
    parser.add_argument('--output_table', default='query-11:rpm.account_id_schema_new',
                        help='input file to write results to.')
    # Parse arguments from the command line.
    # known_args, pipeline_args = parser.parse_known_args(argv)
    args = parser.parse_args()

    dataflow_options = ['--project=%s'%(args.project), '--job_name=%s'%(args.job_name), '--temp_location=%s'%(args.temp_location),'--runner=%s'%(args.runner),
                        '--region=%s'%(args.region)]

    dataflow_options.append('--staging_location=%s'%(args.staging_location))
    options = PipelineOptions(dataflow_options)
    gcloud_options = options.view_as(GoogleCloudOptions)
    #
    options.view_as(StandardOptions).runner = "dataflow"

    input_filename = args.input
    output_filename = args.output

    table_spec = bigquery.TableReference(
        projectId='query-11',
        datasetId='rpm',
        tableId='account_id_schema_new')

    SCHEMA = {"namespace": "example.avro",
              "type": "record",
              "name": "User",
              "fields": [
                  {"name": "ACNO", "type": ["null", {"logicalType": "char", "type": "string", "maxLength": 20}]},
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

    rec_cnt = args.records
    with beam.Pipeline(options=options) as p:
        left_pcol_name = 'p1'
        file = p | 'read_source' >> beam.io.ReadFromAvro('gs://dataflow_s/RPM/account_id_schema_new.avro')
        p1 = file | beam.Map(lambda x: {'ACNO':x['ACNO'],'FIELD_1':x["FIELD_1"]})
        p2 = file | beam.Map(lambda x: {'ACNO': x['ACNO'], 'FIELD_2': x["FIELD_2"]})

        # P1_1 = p1 | "write" >> beam.io.WriteToText('./data.csv')
        # P2_2 = p2 | "write2" >> beam.io.WriteToText('./data2.csv')

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

        pipelines_dictionary = {left_pcol_name: p1, right_pcol_name: p2}
        test_pipeline = pipelines_dictionary | 'left join' >> Join(left_pcol_name=left_pcol_name, left_pcol=p1,
                                                                   right_pcol_name=right_pcol_name, right_pcol=p2,
                                                                   join_type='left', join_keys=join_keys)
        print(type(test_pipeline))

        compressIdc = True
        use_fastavro = True
        #

        table_schema = {
            'fields': [{
                'name': 'ACNO', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'FIELD_1', 'type': 'STRING', 'mode': 'REQUIRED'
            }, {
                'name': 'FIELD_2', 'type': 'STRING', 'mode': 'REQUIRED'
            }]
        }

        test_pipeline | beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

