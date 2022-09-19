from locale import D_T_FMT
from multiprocessing.sharedctypes import Value
from elasticsearch6 import Elasticsearch
from elasticsearch_dsl import Search
from pyunravel import ES
from elasticsearch_dsl import Q
import json
import yaml
from config import *
import config
from datetime import datetime, timedelta
import pytz
import argparse
import pandas as pd
import logging
import sys

logging.basicConfig(filename="interesting_query_script.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')


class InterestingQueries:
    def __init__(self, yaml_dict, args):
        self.est_per_node_peak_memory = yaml_dict['est_per_node_peak_memory']
        self.duration = yaml_dict['duration']
        self.aggregate_peak_memory = yaml_dict['aggregate_peak_memory']
        self.per_node_peak_memory = yaml_dict['per_node_peak_memory']
        self.attributes = yaml_dict['attributes']
        self.rows_produced = yaml_dict['rows_produced']
        self.memory_spilled = yaml_dict['memory_spilled']
        self.admission_wait_time = yaml_dict['admission_wait_time']
        self.hdfs_bytes_read_remote = yaml_dict['hdfs_remote_bytes_read']
        if yaml_dict['statistics_corrupt_or_missing'] == False:
            self.stats_corrupt = 'false'
        elif yaml_dict['statistics_corrupt_or_missing'] == True:
            self.stats_corrupt = 'true'
        else:
            print("------------------ERROR------------------------")
            print("improper value for statistics_corrupt_or_missing")
            print("------------------------------------------------")
            sys.exit()
        self.days = args.days
        if args.days == None:
            self.start_time = datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            self.end_time = datetime.strptime(args.end_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            self.end_time = datetime.now(tz=pytz.utc)
            self.start_time = self.end_time - timedelta(days=args.days)
        if args.user_list != None:
            self.user_list = args.user_list.split(",")
        else:
            self.user_list = None
        if args.pool_list != None:
            self.pool_list = args.pool_list.split(",")
        else:
            self.pool_list = None
        self.out_dict_list = []
        try:
            self.es = ES.from_unravel_properties()
        except:
            print("error loading es from unravel properties!!!")

        self.unravel_url = config.unravel_url

    def check_for_attributes(self, metrics, duration):
        ## agregate, rows produced
        message = ''
        metrics = json.loads(metrics)
        try:
            if int(float(metrics['memory_spilled'])) >= int(float(self.memory_spilled)):
                pass
            else:
                message = 'Memory Spilled was {}, breached the threshold value {}'.format(metrics['memory_spilled'],
                                                                                          self.memory_spilled)
                return False, False
        except:
            logging.info("memory_spilled Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['estimated_per_node_peak_memory'])) >= int(float(self.est_per_node_peak_memory)):
                pass
            else:
                message = 'Estimated Per Node Peak Memory was {}, breached the threshold value {}'.format(
                    metrics['estimated_per_node_peak_memory'], self.est_per_node_peak_memory)
                return False, False
        except:
            logging.info("estimated_per_node_peak_memory Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['memory_per_node_peak'])) >= int(float(self.per_node_peak_memory)):
                pass
            else:
                message = 'Memory Per Node Peak was {}, breached the threshold value {}'.format(
                    metrics['memory_per_node_peak'], self.per_node_peak_memory)
                return False, False
        except:
            logging.info("memory_per_node_peak Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(duration)) >= int(float(self.duration)):
                pass
            else:
                message = 'Query Duration was {}, breached the threshold value {}'.format(duration, self.duration)
                return False, False
        except:
            logging.info("memory_per_node_peak Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['rows_produced'])) >= int(float(self.rows_produced)):
                pass
            else:
                message = 'Rows Produced was {}, breached the threshold value {}'.format(metrics['rows_produced'],
                                                                                         self.rows_produced)
                return False, False
        except:
            logging.info("rows_produced Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['memory_aggregate_peak'])) >= int(float(self.aggregate_peak_memory)):
                pass
            else:
                message = 'Memory Aggregate Peak was {}, breached the threshold value {}'.format(
                    metrics['memory_aggregate_peak'], self.aggregate_peak_memory)
                return False, False
        except:
            logging.info("memory_aggregate_peak Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['admission_wait'])) >= int(float(self.admission_wait_time)):
                pass
            else:
                message = 'Admission Wait was {}, breached the threshold value {}'.format(metrics['admission_wait'],
                                                                                          self.admission_wait_time)
                return False, False
        except:
            logging.info("admission_wait Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['hdfs_bytes_read_remote'])) >= int(float(self.hdfs_bytes_read_remote)):
                pass
            else:
                message = 'HDFS bytes read remote was {}, breached the threshold value {}'.format(
                    metrics['hdfs_bytes_read_remote'], self.hdfs_bytes_read_remote)
                return False, False
        except:
            logging.info("hdfs_bytes_read_remote Key not present in the payload, skipping!!!!!")
            pass
        try:
            if metrics['stats_corrupt'] == self.stats_corrupt:
                pass
            else:
                message = 'Stats Corrupt was {}, not matching with configured value {}'.format(metrics['stats_corrupt'],
                                                                                               self.stats_corrupt)
                return False, False
        except:
            logging.info("stats_corrupt Key not present in the payload, skipping!!!!!")
            pass
        return message, True

    def fetch_data_from_es(self, start_time, end_time):
        search = Search(using=self.es.es, index="app-search")
        search.update_from_dict({
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'type': 'QUERY'
                            },
                        },
                        {
                            'match': {
                                'kind': 'impala'
                            }
                        },
                        {
                            "range": {
                                "startTimeInDate": {
                                    "gte": start_time,
                                    "lte": end_time
                                }
                            },
                        }
                    ]
                }
            }
        })
        try:
            for row in search.scan():
                row_dict = row.to_dict()
                if self.user_list != None and row_dict['userName'] in self.user_list:
                    pass
                elif self.user_list == None:
                    pass
                else:
                    continue
                if self.pool_list != None and row_dict['queue'] in self.pool_list:
                    pass
                elif self.pool_list == None:
                    pass
                else:
                    continue
                try:
                    message, status = self.check_for_attributes(row_dict['metrics'], row_dict['duration'])
                    if status == True:
                        self.out_dict_list.append(row_dict)
                except:
                    logging.info("error at check_for_attributes")
        except:
            logging.info("es error!!!!!")

    def generate_unravel_link(self, query_id, cluster_uuid):
        url = '{}/#/app/application/impala?execId={}&clusterUid={}'.format(self.unravel_url, query_id, cluster_uuid)
        return url

    def run(self):
        self.fetch_data_from_es(self.start_time, self.end_time)
        df_dict_list = []
        for values in self.out_dict_list:
            df_dict = {}
            loaded_metrics = json.loads(values['metrics'])
            df_dict['queryId'] = values['id']
            df_dict['unravelLink'] = self.generate_unravel_link(values['id'], values['clusterUid'])
            df_dict['kind'] = values['kind']
            df_dict['clusterId'] = values['clusterId']
            df_dict['userName'] = values['userName']
            df_dict['queue'] = values['queue']
            df_dict['user'] = values['user']
            df_dict['cpuTime'] = values['cpuTime']
            df_dict['startTime'] = values['startTime']
            df_dict['finishedTime'] = values['finishedTime']
            df_dict['duration'] = values['duration']
            df_dict['memorySeconds'] = values['memorySeconds']
            df_dict['totalProcessingTime'] = values['totalProcessingTime']
            df_dict['storageWaitTime'] = values['storageWaitTime']
            df_dict['memorySpilled'] = loaded_metrics.get('memory_spilled', 'N/A')
            df_dict['rowsProduced'] = loaded_metrics.get('memory_spilled', 'N/A')
            df_dict['estPerNodePeakMemory'] = loaded_metrics.get('estimated_per_node_peak_memory', 'N/A')
            df_dict['perNodePeakMemory'] = loaded_metrics.get('memory_per_node_peak', 'N/A')
            df_dict['aggregatePeakMemory'] = loaded_metrics.get('memory_aggregate_peak', 'N/A')
            df_dict['admissionWaitTime'] = loaded_metrics.get('admission_wait', 'N/A')
            df_dict['hdfsRemoteBytesRead'] = loaded_metrics.get('hdfs_bytes_read_remote', 'N/A')
            df_dict['statisticsCorruptOrMissing'] = loaded_metrics.get('stats_corrupt', 'N/A')
            df_dict_list.append(df_dict)
        df = pd.DataFrame(df_dict_list)
        if df.empty:
            print("--------------------------MESSAGE--------------------------")
            print("No data found for given config!!!                          ")
            print("-----------------------------------------------------------")
        else:
            df.to_csv("out.csv", index=False)
            print("Done and Dusted !!!!!")


if __name__ == "__main__":
    with open("interesting_q_config.yaml", "r") as stream:
        try:
            yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print("------------------ERROR------------------------")
            print("error loading interesting_q_config.yaml file!!!")
            print("------------------------------------------------")
            logging.info(exc)
            sys.exit()

    parser = argparse.ArgumentParser(description='Extract interesting hive apps')
    parser.add_argument('--days', type=int, default=30,
                        help='Get apps within specified number of days in the past from --end-date. Default {} days. '.format(
                            30) +
                             'This is a convenient way to specify --start-date. If --start-date is specified, '
                             '--days is ignored.')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Process apps that started on or after this date/time (inclusive). '
                             'Specify in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format (e.g., 2019-07-01 or 2019-07-01 '
                             '17:13:25). '
                             'Default: --start-date is based on --days if --start-date is not specified.')
    parser.add_argument('--end-date', type=str, default=None,
                        help='Process apps that started before this date/time (exclusive). ' +
                             'Specify in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format (e.g., 2019-07-01 or 2019-07-01 '
                             '17:13:25). '
                             'Note that apps started on this date are excluded. For example, if --end-date is '
                             '2019-07-01, then it will process apps started on or before 2019-06-30 23:59:59, '
                             'but will exclude apps started on or after 2019-07-01 12:00:00. '
                             'Default: the timestamp as of now')
    parser.add_argument('--user-list',
                        help='coma seperated user list')
    parser.add_argument('--pool-list',
                        help='coma saperated pool list')
    args = parser.parse_args()

    if args.start_date:
        try:
            args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except:
            try:
                args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
            except:
                raise ValueError('Invalid start date:', args.start_date)

    if args.end_date:
        try:
            args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        except:
            try:
                args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M:%S')
            except:
                raise ValueError('Invalid end date:', args.end_date)
    InterestingQueries(yaml_dict, args).run()
