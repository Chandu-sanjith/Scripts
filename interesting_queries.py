from elasticsearch6 import Elasticsearch
from elasticsearch_dsl import Search
from pyunravel import ES
from elasticsearch_dsl import Q
import json
import yaml
from datetime import datetime, timedelta
import pytz
import argparse
import pandas as pd
import logging

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
        self.es = ES.from_unravel_properties(props_path="/opt/unravel/data/conf/unravel.properties")
        self.unravel_url = 'https://sd58.unraveldata.com:3000'


    def check_for_attributes(self, metrics, duration):
        ## agregate, rows produced
        metrics = json.dumps(metrics)
        metrics = eval(metrics)
        metrics = json.loads(metrics)
        try:
            if int(float(metrics['memory_spilled'])) >= int(float(self.memory_spilled)):
                pass
            else:
                return False
        except:
            logging.info("memory_spilled Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['estimated_per_node_peak_memory'])) >= int(float(self.est_per_node_peak_memory)):
                pass
            else:
                return False
        except:
            logging.info("estimated_per_node_peak_memory Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['memory_per_node_peak'])) >= int(float(self.per_node_peak_memory)):
                pass
            else:
                return False
        except:
            logging.info("memory_per_node_peak Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(duration)) >= int(float(self.duration)):
                pass
            else:
                return False
        except:
            logging.info("memory_per_node_peak Key not present in the payload, skipping!!!!!")
            pass
        try:
            if int(float(metrics['memory_aggregate_peak'])) >= int(float(self.aggregate_peak_memory)):
                pass
            else:
                return False
        except:
            logging.info("memory_aggregate_peak Key not present in the payload, skipping!!!!!")
            pass
        return True

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
                    if self.check_for_attributes(row_dict['metrics'],row_dict['duration']) == True:
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
            df_dict['QueryId'] = values['id']
            df_dict['UnravelLink'] = self.generate_unravel_link(values['id'],values['clusterUid'])
            df_dict['Kind'] = values['kind']
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
            df_dict_list.append(df_dict)
        df = pd.DataFrame(df_dict_list)
        df.to_csv("out.csv", index=False)
        print("Done and Dusted !!!!!")

if __name__ == "__main__":
    with open("interesting_q_config.yaml", "r") as stream:
        try:
            yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.info(exc)

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
    InterestingQueries(yaml_dict,args).run()
