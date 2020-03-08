from __future__ import absolute_import
import argparse
import re
import csv
import os
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import BigQuerySource

class Dataingestion():
    def parse_method(self,strinput):
        values = re.split(",",re.sub('\r\n', '' ,re.sub(u'"','',strinput)))

        row = dict(zip(('id1','name','date','user_id','class1','tag_based'),values))

        return row


def runit(argv= None):

    parser = argparse.ArgumentParser()

    parser.add_argument('--input', dest='input',required=False,help='Input file is read from local or ',default= 'gs://myhello/result1.csv')


    parser.add_argument('--output', dest= 'output', required=False, help='output BQ table to write results to',default='new1.bigquerydemo')

    dataingestion = Dataingestion()

    args, pipelineknown = parser.parse_known_args(argv)
    p = beam.Pipeline(options=PipelineOptions(pipelineknown))

    (p

    |'Read from a file' >> beam.io.ReadFromText(args.input, skip_header_lines=1)

    |'String to BigQuery Row' >> beam.Map(lambda s: dataingestion.parse_method(s))

    |'Write to Bigquery' >> beam.io.Write(beam.io.BigQuerySink(args.output , schema= 'id1 = STRING,name= STRING,date = STRING, user_id = STRING,class1= STRING,tag_based=STRING',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.runit().wait_until_finish()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    runit()
