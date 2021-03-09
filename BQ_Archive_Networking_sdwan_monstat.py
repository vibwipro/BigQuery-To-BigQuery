#****************************************************************
#Script Name    : BQ_Archive_Networking_sdwanB2BSlamLog.py 
#Description    : This script will load archive data on archive BQ table.
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2021-01-29      Initial version
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
          '--pro_id_sdwanlog',
          dest='pro_id_sdwanlog',
          help='Input Archival table name for sdWanLog.')
    parser.add_argument(
          '--pro_id_monstatlog',
          dest='pro_id_monstatlog',
          help='Input Archival table name for monStat.')
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)

    logging.info('***********')
    data_loading = (
        p1
        | 'ReadBQ-Arc-sdWan' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT C1, applianceName, tenantName, localAccCktName, remoteAccCktName, localSiteName, remoteSiteName, fwdClass, tenantId, delay, fwdDelayVar, revDelayVar, fwdLoss, revLoss, fwdLossRatio, revLossRatio, pduLossRatio, fwdSent, revSent, partition_date FROM `PROJ.Prod_Networking.sdwanB2BSlamLog` WHERE partition_date < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


    project_id = "PROJ"
    dataset_id = 'Prod_Networking_Historical_Data'
    table_id = known_args.pro_id_sdwanlog
    table_schema = ('partition_date:DATETIME, C1:STRING, applianceName:STRING, tenantName:STRING, localAccCktName:STRING, remoteAccCktName:STRING, localSiteName:STRING, remoteSiteName:STRING, fwdClass:STRING, tenantId:INTEGER, delay:INTEGER, fwdDelayVar:INTEGER, revDelayVar:INTEGER, fwdLoss:INTEGER, revLoss:INTEGER, fwdLossRatio:STRING, revLossRatio:STRING, pduLossRatio:STRING, fwdSent:INTEGER, revSent:INTEGER')

        # Persist to BigQuery
        # WriteToBigQuery accepts the data as list of JSON objects

#---------------------Type = audit----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-sdWan' >> beam.io.WriteToBigQuery(
                                                    table=table_id,
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#--------------------------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------------------------

    data_loading = (
        p1
        | 'Read-Arc-monStat' >> beam.io.Read(beam.io.BigQuerySource(query='''SELECT mstatsTimeBlock_datetime, C1, applianceName, tenantName, mstatsTimeBlock, tenantId, vsnId, mstatsTotSentOctets, mstatsTotRecvdOctets, mstatsTotSessDuration, mstatsTotSessCount, mstatsType, appId, site, accCkt, siteId, accCktId, user, risk, productivity, family, subFamily, bzTag  FROM `PROJ.Prod_Networking.monStatsLog_app_stats` WHERE mstatsTimeBlock_datetime < DATETIME_SUB("''' +known_args.datetime + '''", INTERVAL 30 DAY); ''', use_standard_sql=True))
    )


    project_id = "PROJ"
    dataset_id = 'Prod_Networking_Historical_Data'
    table_id = known_args.pro_id_monstatlog
    table_schema = ('mstatsTimeBlock_datetime:DATETIME, C1:STRING, applianceName:STRING, tenantName:STRING, mstatsTimeBlock:INTEGER, tenantId:INTEGER, vsnId:INTEGER, mstatsTotSentOctets:INTEGER, mstatsTotRecvdOctets:INTEGER, mstatsTotSessDuration:INTEGER, mstatsTotSessCount:INTEGER, mstatsType:STRING, appId:STRING, site:STRING, accCkt:STRING, siteId:INTEGER, accCktId:INTEGER, user:STRING, risk:INTEGER, productivity:INTEGER, family:STRING, subFamily:STRING, bzTag:STRING')

        # Persist to BigQuery
        # WriteToBigQuery accepts the data as list of JSON objects

#---------------------Type = audit----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Write-Arc-monStat' >> beam.io.WriteToBigQuery(
                                                    table=table_id,
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
  path_service_account = '/home/vibhg/PROJ-fb-abc-54dh-c927.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()

