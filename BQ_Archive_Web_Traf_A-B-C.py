#****************************************************************
#Script Name    : BQ_Archive_Web_Traf_A-B-C.py
#Description    : This script will load archive data on archive BQ table.
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2021-02-23      Initial version
#****************************************************************

#------------Import Lib-----------------------#
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam as beam
from apache_beam import window
import os, sys, argparse, logging
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime

#------------Set up BQ parameters-----------------------#
# Replace with Project Id
project = 'PROJ'

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
          '--datetime',
          dest='datetime',
          help='Input date time to process.')
    parser.add_argument(
          '--pro_id_prefix',
          dest='pro_id_prefix',
          help='Input prefix for Archival table.')
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)

    logging.info('***********')
    data_loading = (
        p1
        | 'ReadBQ-Arc-AT' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT id, reqid, guid, type, cp, start, processedTime, protoVer, cliIP, reqMethod, respCT, t.proto, rrerere, UA, reqPath, respLen, status, reqHost, referer, reqTime, country, date, respCacheCtl, allowOrigin, midMileLatency, errCdF29, lastByte, clientRTT, asnum, errCdR14, downloadStatus, netOriginLatency, downloadTime, edgeIP, _bkt, _cd, _indextime, _serial, _sourcetype, _time, host, index, linecount, source, sourcetype, splunk_server FROM `PROJ.Prod_Splunk.Akamai_Checkout_Cart_AT` t WHERE date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


    project_id = "PROJ"
    dataset_id = 'Prod_Splunk_Historical_Data'
    table_schema = ('id:STRING, reqid:STRING, guid:STRING, type:STRING, cp:STRING, start:STRING, processedTime:STRING, protoVer:STRING, cliIP:STRING, reqMethod:STRING, respCT:STRING, proto:STRING, rrerere:STRING, UA:STRING, reqPath:STRING, respLen:STRING, status:STRING, reqHost:STRING, referer:STRING, reqTime:STRING, country:STRING, date:DATETIME, respCacheCtl:STRING, allowOrigin:STRING, midMileLatency:STRING, errCdF29:STRING, lastByte:STRING, clientRTT:STRING, asnum:STRING, errCdR14:STRING, downloadStatus:STRING, netOriginLatency:STRING, downloadTime:STRING, edgeIP:STRING, _bkt:STRING, _cd:STRING, _indextime:STRING, _serial:STRING, _sourcetype:STRING, _time:STRING, host:STRING, index:STRING, linecount:STRING, source:STRING, sourcetype:STRING, splunk_server:STRING')
        # Persist to BigQuery
        # WriteToBigQuery accepts the data as list of JSON objects

#---------------------Country = AT----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-AT' >> beam.io.WriteToBigQuery(
                                                    table=known_args.pro_id_prefix + '_Akamai_Checkout_Cart_AT',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------Country = AU--------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------------------------

    data_loading = (
        p1
        | 'Read-Arc-AU' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT id, reqid, guid, type, cp, start, processedTime, protoVer, cliIP, reqMethod, respCT, t.proto, rrerere, UA, reqPath, respLen, status, reqHost, referer, reqTime, country, date, respCacheCtl, allowOrigin, midMileLatency, errCdF29, lastByte, clientRTT, asnum, errCdR14, downloadStatus, netOriginLatency, downloadTime, edgeIP, _bkt, _cd, _indextime, _serial, _sourcetype, _time, host, index, linecount, source, sourcetype, splunk_server FROM `PROJ.Prod_Splunk.Akamai_Checkout_Cart_AU` t WHERE date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


#---------------------Type = AU------------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-AU' >> beam.io.WriteToBigQuery(
                                                    table=known_args.pro_id_prefix + '_Akamai_Checkout_Cart_AU',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))

#-----------------------------------------------Country = BE---------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------------------------

    data_loading = (
        p1
        | 'Read-Arc-BE' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT id, reqid, guid, type, cp, start, processedTime, protoVer, cliIP, reqMethod, respCT, t.proto, rrerere, UA, reqPath, respLen, status, reqHost, referer, reqTime, country, date, respCacheCtl, allowOrigin, midMileLatency, errCdF29, lastByte, clientRTT, asnum, errCdR14, downloadStatus, netOriginLatency, downloadTime, edgeIP, _bkt, _cd, _indextime, _serial, _sourcetype, _time, host, index, linecount, source, sourcetype, splunk_server FROM `PROJ.Prod_Splunk.Akamai_Checkout_Cart_BE` t WHERE date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


#---------------------Type = BE--------------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-BE' >> beam.io.WriteToBigQuery(
                                                    table=known_args.pro_id_prefix + '_Akamai_Checkout_Cart_BE',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))

#-----------------------------------------------Country = CA---------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------------------------

    data_loading = (
        p1
        | 'Read-Arc-CA' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT id, reqid, guid, type, cp, start, processedTime, protoVer, cliIP, reqMethod, respCT, t.proto, rrerere, UA, reqPath, respLen, status, reqHost, referer, reqTime, country, date, respCacheCtl, allowOrigin, midMileLatency, errCdF29, lastByte, clientRTT, asnum, errCdR14, downloadStatus, netOriginLatency, downloadTime, edgeIP, _bkt, _cd, _indextime, _serial, _sourcetype, _time, host, index, linecount, source, sourcetype, splunk_server FROM `PROJ.Prod_Splunk.Akamai_Checkout_Cart_CA` t WHERE date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


#---------------------Type = CA--------------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-CA' >> beam.io.WriteToBigQuery(
                                                    table=known_args.pro_id_prefix + '_Akamai_Checkout_Cart_CA',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))

#-----------------------------------------------Country = CH---------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------------------------

    data_loading = (
        p1
        | 'Read-Arc-CH' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT id, reqid, guid, type, cp, start, processedTime, protoVer, cliIP, reqMethod, respCT, t.proto, rrerere, UA, reqPath, respLen, status, reqHost, referer, reqTime, country, date, respCacheCtl, allowOrigin, midMileLatency, errCdF29, lastByte, clientRTT, asnum, errCdR14, downloadStatus, netOriginLatency, downloadTime, edgeIP, _bkt, _cd, _indextime, _serial, _sourcetype, _time, host, index, linecount, source, sourcetype, splunk_server FROM `PROJ.Prod_Splunk.Akamai_Checkout_Cart_CH` t WHERE date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


#---------------------Type = CH--------------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-CH' >> beam.io.WriteToBigQuery(
                                                    table=known_args.pro_id_prefix + '_Akamai_Checkout_Cart_CH',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))


    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  path_service_account = '/home/vibhg/PROJ-fbabc-87927.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()

