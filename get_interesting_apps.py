#!/usr/bin/env python
"""
script used to fetch apps that have tuning recommendations and write data to an output file in JSON format
or CSV format (including scores). Extracting info for Hive (on MR and Tez), Spark and MR is currently supported.
Usage: https://github.com/unraveldata-org/unravel-utilities/blob/master/interesting-apps/README.md
"""
import argparse
import base64
import gzip
import json
import os
import subprocess
import sys
import traceback
import pandas as pd
from abc import abstractmethod
from datetime import datetime, timedelta

sys.path.insert(1, 'pyunravel-packages')
from elasticsearch_dsl import Search
from pyunravel import ES, DB
from io import StringIO as IO
from collections import OrderedDict
from time import time
from tqdm import tqdm

DEFAULT_NUM_DAYS = 30
DEFAULT_SORT_KEY = 'memorySeconds'
DEFAULT_SORT_ORDER = 'desc'
VERBOSE = False


class AppExtractor(object):
    MAX_FETCH_COUNT = 1000
    SKIPPED_EVENT_CONF = ['@class', 'vid', 'va', 'vt', 'vn', 'vi', 'yg', 'yt', 'yid', 'usr', 'q', 'cl', 'sr', 'dr',
                          'tg', 'cf', 'aid']

    def __init__(self, args):
        self.app_type = args.app_type
        self.out_extract_file = args.out_extract_file
        self.out_score_file = args.out_score_file
        self.stages = args.stages
        self.min_duration_seconds = args.min_duration_seconds
        self.min_vcore_seconds = args.min_vcore_seconds
        self.min_memory_seconds = args.min_memory_seconds
        self.score_algo = args.score_algo
        self.score_params = args.score_params
        self.start_ts, self.end_ts = self._get_start_and_end_timestamp_str(args.start_date, args.end_date, args.days)
        self.max_apps = args.max_apps
        self.with_request_user = args.with_request_user
        self.with_insights = args.with_insights
        self.with_full_annotation = args.with_full_annotation
        self.sort_key = args.sort_key
        self.sort_order = args.sort_order
        #self.db = DB.from_unravel_properties(props_path="/opt/unravel/data/conf/unravel.properties")
        self.db = DB("jdbc:postgresql://127.0.0.1:4339/unravel", username="unravel", password="ve9NEEyDj7ArewVl5hyu5W3kDja8xuMUOVTSEpjJFoKNnw6Z1GpyA88mJOn1QAwI")
        try:
            self.es = ES()
        except:
            raise Exception("Could not load DB or ES from unravel.properties")

        self.print_memory_usage = args.print_memory_usage
        self.cluster_uid = args.cluster_uid

    def _get_start_and_end_timestamp_str(self, start_time, end_time, days):
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(days=days)

        if start_time > end_time:
            raise ValueError('start time {} occurs after end time {}'.format(start_time, end_time))

        return [ts.strftime('%Y-%m-%d %H:%M:%S.%f') for ts in [start_time, end_time]]

    def get_event_instances_data(self, count=False):
        event_type = 'SU'  # EventConstants.EventType.SUMMARY
        clause = "FROM event_instances WHERE entity_type='{}' AND created_at>='{}' AND created_at<'{}' AND latest=1".format(
            self.entity_type, self.start_ts, self.end_ts)

        if self.entity_type == 2:
            # For Spark apps, some recommndations are not of event type 'SU'
            # Eg. ContainerSizingUnderutilizationEvent is of event type 'IA'
            query = "SELECT distinct event_name FROM event_instances"
            response = self.db.execute(query)
            event_names = [i[0] for i in response]
            #event_names = ['ContainerSizingUnderutilizationEvent']
            clause += " AND (event_type='{}' OR event_name IN ({}))".format(event_type, ','.join(
                ("'%s'" % (event_name) for event_name in event_names)))
        else:
            clause += " AND event_type='{}'".format(event_type)

        # For Hive on Tez, SU event_instances are repeated 3 times
        # (one each for '<username>_*', 'application_*', 'dag_*' entity_id)
        if self.entity_type == 15:
            clause += " AND entity_id NOT LIKE 'dag_%%' AND entity_id NOT LIKE 'application_%%'"

        if self.cluster_uid:
            clause += " AND cluster='{}'".format(self.cluster_uid)

        if count:
            query = 'SELECT count(*) {}'.format(clause)
            return self.db.execute(query)[0][0]
        else:
            order = 'ORDER BY created_at DESC'
            limit = ' LIMIT {}'.format(self.max_apps) if self.max_apps > 0 else ''
            query = 'SELECT entity_id, json {} {} {}'.format(clause, order, limit)
            return self.db.execute_in_batch(query)

    def get_all_event_instances_data(self, entity_id):
        event_type = 'SU'  # EventConstants.EventType.SUMMARY
        query = "SELECT json FROM event_instances WHERE entity_id='{}' AND latest=1 AND event_type <> '{}'".format(
            entity_id, event_type)
        return self.db.execute(query)

    def read_test_data(self):
        rows = []
        with open('rec_apps.log') as f:
            for line in f:
                print(line.rstrip('\n'))
                rows.append(json.loads(line.rstrip('\n')))
        return rows

    def get_apps_with_recommendations(self):
        apps = OrderedDict()
        rows = self.get_event_instances_data()
        for row in rows:
            entity_id = row[0]
            if entity_id not in apps:
                apps[entity_id] = OrderedDict()
                self.initialize_app_data(entity_id, apps[entity_id])
            rec_json = json.loads(row[1])
            if rec_json['usr']:
                apps[entity_id]['user'] = rec_json['usr']
            if rec_json['q']:
                apps[entity_id]['queue'] = rec_json['q']
            for rec in rec_json['cf']:
                apps[entity_id]['recommendations'].append({'event': rec_json['va'],
                                                           'conf': rec,
                                                           'currentValue': rec_json['cf'][rec]['cset'],
                                                           'recommendedValue': rec_json['cf'][rec]['nset']})
        return apps

    def populate_insights_data(self, entity_id, app_data):
        rows = self.get_all_event_instances_data(entity_id)
        if 'insights' not in app_data:
            app_data['insights'] = []
        for row in rows:
            rec_json = json.loads(row[0])
            event = dict()
            event['event'] = rec_json['va']
            for conf in rec_json:
                if conf not in self.SKIPPED_EVENT_CONF:
                    event[conf] = rec_json[conf]
            app_data['insights'].append(event)

    @abstractmethod
    def initialize_app_data(self, entity_id, app_data):
        raise NotImplementedError('initialize_app_data must be overridden')

    @abstractmethod
    def extract_app_data(self, entity_id, app_data):
        raise NotImplementedError('extract_app_data must be overridden')

    def convert_json_to_csv_and_write_to_file(self, all_app_data, out_file):
        df = pd.json_normalize(all_app_data, "recommendations" , ["id","user","queue","startTime","duration","memorySeconds","vcoreSeconds"])
        df.to_csv(out_file, index=False)


    def write_to_extract_file(self, all_app_data, out_file):
        if out_file.endswith('.csv'):
            self.convert_json_to_csv_and_write_to_file(all_app_data,out_file)
        else:
            with open(out_file, 'w') as fd:
                fd.write(json.dumps(all_app_data, indent=4))

    def filter_apps(self, apps):
        keys = ['id', 'user', 'queue', 'memorySeconds', 'vcoreSeconds', 'duration', 'recommendations', 'mrJobIds']

        filtered_apps = []

        for app in apps:
            app_dict = {k: app.get(k, '') for k in keys}
            app_dict['numRecs'] = len(app_dict['recommendations'])
            app_dict['numMrTasks'] = len(app_dict.get('mrJobIds', []))
            if app_dict['duration'] >= self.min_duration_seconds * 1000 and (
                    app_dict['vcoreSeconds'] >= self.min_vcore_seconds or app_dict[
                'memorySeconds'] >= self.min_memory_seconds):
                filtered_apps.append(app_dict)
        return filtered_apps

    def score_apps(self, apps, algo='ale'):
        if algo != 'ale':
            raise ValueError('Unsupported scoring algo {}'.format(algo))

        try:
            import pandas as pd
            from scipy.stats import norm
        except ImportError as e:
            print('Could not perform app scoring due to missing dependencies:')
            print(e)

        score_params = [int(param_str) for param_str in self.score_params.split(',')]
        SKEW_WEIGHT, SPLIT_WEIGHT, SKEW_SPLIT_WEIGHT, NUM_RECS_WEIGHT, HIGH_DURATION_WEIGHT = score_params
        SKEW_SPLIT_WEIGHT_DELTA = SKEW_SPLIT_WEIGHT - SKEW_WEIGHT - SPLIT_WEIGHT

        print('Using scoring algorithm {} with scoring params {}'.format(self.score_algo, score_params))

        df = pd.DataFrame(apps)

        skews = df.recommendations.apply(lambda r: 'skew detected' in repr(r).lower())
        splits = df.recommendations.apply(lambda r: 'mapreduce.input.fileinputformat.split.maxsize' in repr(r).lower())

        if df.numRecs.std() > 0:
            num_recs_scores = norm.cdf(df.numRecs, df.numRecs.mean(), df.numRecs.std()) * NUM_RECS_WEIGHT
        else:
            num_recs_scores = [NUM_RECS_WEIGHT] * len(df.numRecs)

        if df.duration.std() > 0:
            high_duration_scores = norm.cdf(df.duration, df.duration.mean(), df.duration.std()) * HIGH_DURATION_WEIGHT
        else:
            high_duration_scores = [HIGH_DURATION_WEIGHT] * len(df.duration)
        skew_split_scores = [skew * SKEW_WEIGHT + split * SPLIT_WEIGHT + skew * split * SKEW_SPLIT_WEIGHT_DELTA for
                             skew, split in zip(skews, splits)]

        scores = num_recs_scores + high_duration_scores + skew_split_scores

        df['skew'] = skews
        df['split'] = splits
        df['score'] = scores.round(2)
        df = df.sort_values(by='score', ascending=False)

        return df

    def _print_header(self, header):
        print('-' * len(header))
        print(header)
        print('-' * len(header))

    def run(self):
        if self.stages in ['all', 'extract']:
            self._print_header('Extract {} apps with recommendations'.format(self.app_type))
            app_count = self.get_event_instances_data(count=True)
            if (self.max_apps > 0) and (app_count > self.max_apps):
                print(
                    'There are {} {} apps with recommendations, but limiting to {} of them. You can modify '
                    '--max-apps to change the number of apps to be processed.'.format(
                        app_count, self.app_type, self.max_apps))
                app_count = self.max_apps
            elif app_count > 1000000:
                print(
                    '[warning] There are {} {} apps with recommendations (more than 1,000,000). Consider using '
                    '--max-apps to reduce the number of apps to be processed.'.format(
                        self.app_type, app_count))

            t1 = time()
            print('Retrieving {} {} apps with recommendations from database for cluster with UID: {}...'.format(app_count,
                                                                                                              self.app_type,
                                                                                                              self.cluster_uid))
            sys.stdout.flush()
            rec_apps = self.get_apps_with_recommendations()
            t2 = time()
            print('Done (Took {:.1f}s)'.format(t2 - t1))
            print("Processing {} apps".format(len(rec_apps)))

            def get_current_memory_usage():
                """ Memory usage in kB """

                with open('/proc/self/status') as f:
                    memusage = f.read().split('VmRSS:')[1].split('\n')[0][:-3]

                return int(memusage.strip())

            for i, entity_id in enumerate(tqdm(rec_apps)):
                try:
                    if self.with_insights:
                        self.populate_insights_data(entity_id, rec_apps[entity_id])
                    self.extract_app_data(entity_id, rec_apps[entity_id])
                    if self.print_memory_usage and (i % 1000 == 0):
                        print('VmRSS: {}MB'.format(get_current_memory_usage() / 1024))
                except Exception as e:
                    print('Skipping app {}; failed to process due to {}'.format(entity_id, e))
                    continue

            # print('memory footprint size=', sys.getsizeof(processed_apps))
            print('Valid apps: {}'.format(len(rec_apps)))
            rec_apps = sorted(list(rec_apps.values()), key=lambda x: int(x[self.sort_key]),
                              reverse=self.sort_order == 'desc')
            print('Writing apps with recommendations (extract stage) to {}...'.format(self.out_extract_file))
            # print(os.path.abspath(self.out_extract_file))
            self.write_to_extract_file(rec_apps, self.out_extract_file)
            print('Done')

        if self.stages in ['all', 'score']:
            self._print_header('Filter and score {} apps with recommendations'.format(self.app_type))
            if 'rec_apps' not in vars():
                with open(self.out_extract_file) as f:
                    rec_apps = json.load(f)
            rec_apps = self.filter_apps(rec_apps)
            print('There are {} {} apps after filtering'.format(len(rec_apps), self.app_type))
            if len(rec_apps) == 0:
                print('There are no apps to tune. Exiting')
                sys.exit(1)
            df = self.score_apps(rec_apps)
            print('Writing score file to {}...'.format(self.out_score_file))
            df.to_csv(self.out_score_file, index=False, columns=[
                'id', 'user', 'queue', 'memorySeconds', 'vcoreSeconds', 'recommendations', 'mrJobIds', 'numMrTasks',
                'duration', 'numRecs', 'skew', 'split', 'score'])
            print('Done')


class HiveAppExtractor(AppExtractor):
    """
    Fetch data from ES & DB for Hive apps with recommendations for last N (default 30) days
    The output contains:
    * appId
    * user
    * requestUser
    * queue
    * queryString
    * startTime
    * duration
    * memory-seconds
    * vcore-seconds
    * full list of recommendations
    Sample output:
    [
      {
        "id": "application_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
        "user": "test1",
        "requestUser": "test1",
        "queryString": "select dt.d_year ,item.i_brand_id brand_id ,item.i_brand brand ,sum(ss_ext_sales_price) sum_agg from date_dim dt ,store_sales ,item where dt.d_date_sk = store_sales.ss_sold_date_sk and store_sales.ss_item_sk = item.i_item_sk and item.i_manufact_id = 436 and dt.d_moy=12 group by dt.d_year ,item.i_brand ,item.i_brand_id order by dt.d_year ,sum_agg desc ,brand_id limit 100",
        "mrJobIds": [
          "job_1631609793721_0255",
          "job_1631609793721_0257"
        ],
        "queue": "default",
        "startTime": "2021-09-19T10:25:50.895Z",
        "duration": 85108,
        "memorySeconds": 1583254,
        "vcoreSeconds": 148,
        "conf": {},
        "recommendations": [
          {
            "recommendedValue": "523",
            "currentValue": "12288",
            "event": "SummaryEvent",
            "conf": "mapreduce.reduce.memory.mb"
          },
          {
            "recommendedValue": "1406",
            "currentValue": "6144",
            "event": "SummaryEvent",
            "conf": "mapreduce.map.memory.mb"
          },
          {
            "recommendedValue": "-Xmx419m",
            "currentValue": "-Xmx9830m",
            "event": "SummaryEvent",
            "conf": "mapreduce.reduce.java.opts"
          },
          {
            "recommendedValue": "-Xmx1125m",
            "currentValue": "-Xmx4915m",
            "event": "SummaryEvent",
            "conf": "mapreduce.map.java.opts"
          }
        ],
        "insights": [
          {
            "nmt": 5,
            "amm": 979,
            "cmm": 6144,
            "paid": "application_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
            "event": "HiveTooLargeMapEvent",
            "smm": 1406,
            "smj": 1125,
            "cuid": "j-3GD1TNHPQ1EPZ",
            "tmm": 1406,
            "cym": 32
          },
          {
            "trm": 523,
            "srj": 419,
            "crm": 12288,
            "paid": "application_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
            "srm": 523,
            "cuid": "j-3GD1TNHPQ1EPZ",
            "amr": 365,
            "event": "HiveTooLargeReduceEvent",
            "cym": 32,
            "nrt": 5
          },
          {
            "lji": "job_1631609793721_0255",
            "nj": 2,
            "hep": false,
            "qst": 1632047150895,
            "ljd": 47510,
            "paid": "application_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
            "hqi": [
              "map and reduce tasks request too much memory"
            ],
            "qet": 1632047236003,
            "cuid": "j-3GD1TNHPQ1EPZ",
            "fjs": 1632047161598,
            "event": "HiveTimeBreakdownEvent",
            "ljis": []
          }
        ],
        "totalMapSlotDuration": 41986,
        "totalReduceSlotDuration": 17554,
        "tezId": "2021091909T090801Z--4734666018616092409"
      }
    ]
    """

    def __init__(self, args):
        super(HiveAppExtractor, self).__init__(args)
        self.entity_type = 1

    def get_tez_annotation(self, tez_id):
        query = "SELECT detail1 FROM blackboards WHERE entity_id='{}' AND entity_type='app'".format(tez_id)
        return self.db.execute(query)

    def get_hive_query_annotation_from_es(self, entity_id, app_data):
        try:
            search = Search(using=self.es.es, index="app-search").query("match", id=entity_id)
            row = next(search.scan())
            if row:
                row = row.to_dict()
                app_data['startTime'] = row.get('startTime', 0)
                app_data['duration'] = row.get('duration', 0)
                # Due to https://unraveldata.atlassian.net/projects/HIVE/issues/HIVE-105, hive_queries.annotation
                # contains `memorySeconds` and `vcoreSeconds` fields, but the figures are not rolled up.
                # We do not want to double count once HIVE-105 is fixed, so ignore and always do client-side
                # roll up for now to keep the logic simple.
                app_data['memorySeconds'] = 0
                app_data['vcoreSeconds'] = 0
                app_data['totalMapSlotDuration'] = row.get('totalMapSlotDuration', 0)
                app_data['totalReduceSlotDuration'] = row.get('totalReduceSlotDuration', 0)
                app_data['queryString'] = row.get('queryStringFull', "")
                app_data['mrJobIds'] = row.get('mrJobIds', [])
                app_data['tezId'] = row.get('appid', "")
                # realUser was added in 4.5.3 via https://unraveldata.atlassian.net/projects/HIVE/issues/HIVE-92
                app_data['requestUser'] = row.get('user', '-')
                if self.with_full_annotation:
                    app_data['annotation'] = {k: v for k, v in list(row.items()) if k not in
                                              ['user', 'queue', 'startTime', 'duration', 'memorySeconds',
                                               'vcoreSeconds',
                                               'totalMapSlotDuration', 'totalReduceSlotDuration', 'queryStringFull',
                                               'mrJobIds']}
        except Exception as e:
            print('ERROR while decoding hive query annotation for query: ' + app_data['id'], e)
        return app_data['mrJobIds'], app_data['tezId'], app_data['requestUser']

    def get_mr_jobs_annotations_from_es(self, mr_job_ids, app_data):
        try:
            search = Search(using=self.es.es, index="app-search").query("terms", id=mr_job_ids)
            for row in search.scan():
                row = row.to_dict()
                app_data['memorySeconds'] += row.get('memorySeconds', 0)
                app_data['vcoreSeconds'] += row.get('vcoreSeconds', 0)
        except Exception as e:
            print('ERROR while decoding mr job annotation for query: ' + app_data['id'], e)

    def decode_tez_annotation(self, row, app_data):
        try:
            annotation = json.loads(decode_base64_and_gzip_decompress(row[0][0]))
            app_data['memorySeconds'] += annotation.get('memorySeconds', 0)
            app_data['vcoreSeconds'] += annotation.get('vcoreSeconds', 0)
        except Exception as e:
            print('ERROR while decoding tez annotation for query: ' + app_data['id'], e)

    def initialize_app_data(self, entity_id, app_data):
        app_data['id'] = entity_id
        app_data['user'] = '-'
        app_data['requestUser'] = '-'  # hive only
        app_data['queryString'] = '-'  # hive only
        app_data['mrJobIds'] = []      # hive only
        app_data['queue'] = '-'
        app_data['startTime'] = 0
        app_data['duration'] = 0
        app_data['memorySeconds'] = 0
        app_data['vcoreSeconds'] = 0
        app_data['conf'] = {}
        app_data['recommendations'] = []

    def extract_app_data(self, entity_id, app_data):
        mr_job_ids, tez_id, request_user = self.get_hive_query_annotation_from_es(entity_id, app_data)
        if len(mr_job_ids) > 0:
            self.get_mr_jobs_annotations_from_es(mr_job_ids, app_data)
        elif len(tez_id) > 0:
            row = self.get_tez_annotation(tez_id)
            self.decode_tez_annotation(row, app_data)


class HiveOnTezAppExtractor(HiveAppExtractor):
    def __init__(self, args):
        super(HiveOnTezAppExtractor, self).__init__(args)
        self.entity_type = 15


class SparkAppExtractor(AppExtractor):
    """
    Fetch data from ES & DB for Spark apps with recommendations for last N (default 30) days
    The output contains:
    * appId
    * user
    * queue
    * startTime
    * duration
    * memory-seconds
    * vcore-seconds
    * spark conf parameters used by recommendations
      (those apps with no memory savings)
    * full list of recommendations
    Sample output:
    [
        {
            "id": "application_1631609793721_0930",
            "user": "-",
            "queue": "-",
            "startTime": "2021-10-14T12:04:26.881Z",
            "duration": 768126,
            "memorySeconds": 4114582,
            "vcoreSeconds": 1532,
            "conf": {
                "spark.executor.cores": "1",
                "spark.default.parallelism": "2",
                "spark.dynamicAllocation.enabled": "false",
                "spark.driver.memory": "2048M",
                "spark.app.name": "SLA_Bound_App_Perf_Improvement_1634213060_BeforeUnravel",
                "spark.executor.instances": "1",
                "spark.executor.memory": "4g",
                "spark.shuffle.service.enabled": "true"
            },
            "recommendations": [
                {
                    "recommendedValue": "2242289664",
                    "currentValue": "4294967296",
                    "event": "ContainerSizingUnderutilizationEvent",
                    "conf": "spark.executor.memory"
                }
            ],
            "insights": [
                {
                    "tableHeader": "<table class=\"table tables white-table table-condensed clickable-table es-search-results-6 ng-scope\"><thead><tr><th>Parameter</th><th>Current Value</th><th>Recommended Value</th></tr></thead>",
                    "resourceUnderutilizationText": "memory resources",
                    "partitioningText": "",
                    "paid": null,
                    "oldSettings": "spark.executor.memory=4294967296",
                    "suggestedSettings": "spark.executor.memory=2242289664",
                    "cuid": "j-3GD1TNHPQ1EPZ",
                    "event": "ContainerSizingUnderutilizationEvent"
                },
                {
                    "tableHeader": "<table class=\"component-data-tables row-hover\"><thead><tr><th>Parameter</th><th>Current Value</th><th>Recommended Value</th></tr></thead>",
                    "oldSettings": "spark.executor.memory=4294967296,spark.executor.cores=1",
                    "description": "",
                    "paid": null,
                    "suggestedSettings": "spark.executor.memory=3737149440,spark.executor.cores=2",
                    "estimatedImprovement": 30,
                    "cuid": "j-3GD1TNHPQ1EPZ",
                    "event": "ConfigHintEvent"
                },
                {
                    "tableHeader": "<table class=\"table tables white-table table-condensed clickable-table es-search-results-6 ng-scope\"><thead><tr><th>Parameter</th><th>Current Value</th><th>Recommended Value</th></tr></thead>",
                    "sparkAppReport": {
                        "appTimeDistributions": {
                            "queueWaitTime": 4008,
                            "totalFileWriteSetupTime": 39,
                            "totalJobTime": 752780,
                            "totalFileWriteCommitTime": 215,
                            "totalDriverTime": 20670,
                            "totalAppTime": 773450
                        },
                        "appTaskTimeDetail": {
                            "inputStageDetail": {
                                "taskTimeDetail": {
                                    "totalShuffleReadRemote": 0,
                                    "dataSanTime": 122,
                                    "totalDataRead": 2680,
                                    "gcTime": 0,
                                    "totalFetchWaitTime": 0,
                                    "unAccountedTime": 10,
                                    "cpuTime": 136,
                                    "totalShuffleWriteTime": 0,
                                    "totalShuffleWrite": 118,
                                    "totalDataWritten": 0,
                                    "totalShuffleReadLocal": 0,
                                    "resultSerializationTime": 0,
                                    "scheduleWaitTime": 6,
                                    "executorDeserializeTime": 31,
                                    "totalShuffleRead": 0,
                                    "executorRunTime": 146
                                },
                                "taskTime": 183,
                                "topStages": [
                                    16
                                ],
                                "numTasks": 2,
                                "insights": [
                                    "1. StageId 16 has inefficient partition size 1 KB while reading.",
                                    "2. Input stages spent more than 50% of task time on CPU."
                                ]
                            },
                            "insights": [
                                "Wall clock time of processing stages is more than 30% of overall stage wall clock time."
                            ],
                            "outputStageDetail": {
                                "taskTimeDetail": {
                                    "totalShuffleReadRemote": 0,
                                    "dataSanTime": 0,
                                    "totalDataRead": 0,
                                    "gcTime": 0,
                                    "totalFetchWaitTime": 0,
                                    "unAccountedTime": 333,
                                    "cpuTime": 1053,
                                    "totalShuffleWriteTime": 0,
                                    "totalShuffleWrite": 0,
                                    "totalDataWritten": 31394,
                                    "totalShuffleReadLocal": 0,
                                    "resultSerializationTime": 4,
                                    "scheduleWaitTime": 59,
                                    "executorDeserializeTime": 265,
                                    "totalShuffleRead": 0,
                                    "executorRunTime": 1386
                                },
                                "taskTime": 1714,
                                "topStages": [
                                    5,
                                    6,
                                    7
                                ],
                                "numTasks": 22,
                                "insights": [
                                    "1. StageId 6 has inefficient partition size 1 KB while writing.",
                                    "2. Output stages spent more than 50% of task time on CPU."
                                ]
                            },
                            "processStageDetail": {
                                "taskTimeDetail": {
                                    "totalShuffleReadRemote": 0,
                                    "dataSanTime": 0,
                                    "totalDataRead": 0,
                                    "gcTime": 651,
                                    "totalFetchWaitTime": 20,
                                    "unAccountedTime": -639,
                                    "cpuTime": 746479,
                                    "totalShuffleWriteTime": 1685,
                                    "totalShuffleWrite": 1893754,
                                    "totalDataWritten": 0,
                                    "totalShuffleReadLocal": 7548272,
                                    "resultSerializationTime": 15,
                                    "scheduleWaitTime": 1698,
                                    "executorDeserializeTime": 1249,
                                    "totalShuffleRead": 7548272,
                                    "executorRunTime": 747545
                                },
                                "taskTime": 750507,
                                "topStages": [
                                    0,
                                    1,
                                    3
                                ],
                                "numTasks": 603,
                                "insights": [
                                    "1. StageId 1 has inefficient partition size 18 KB while shuffling.",
                                    "2. Processing stages spent more than 50% of task time on CPU."
                                ]
                            },
                            "totalTaskTime": 752404
                        }
                    },
                    "event": "SparkAppTimeReport",
                    "paid": null,
                    "cuid": "j-3GD1TNHPQ1EPZ"
                },
                {
                    "rightRows": -1,
                    "joinedRows": 40000000000,
                    "db": null,
                    "paid": null,
                    "tn": null,
                    "leftRows": -1,
                    "cuid": "j-3GD1TNHPQ1EPZ",
                    "queryid": null,
                    "joinCondition": "id#12L = id#2L",
                    "tablesInvolved": [],
                    "event": "InefficientJoinConditionEvent"
                }
            ]
        }
    ]
    """

    REC_CONFS = {
        "spark.app.name": "-",
        "spark.executor.memory": 1073741824,
        "spark.driver.memory": 1073741824,
        "spark.executor.cores": 1,
        "spark.driver.cores": 1,
        "spark.yarn.driver.memoryOverhead": 384,
        "spark.yarn.am.memoryOverhead": 384,
        "spark.yarn.executor.memoryOverhead": 384,
        "spark.default.parallelism": 2,
        "spark.executor.instances": 2,
        "spark.dynamicAllocation.enabled": 0,
        "spark.dynamicAllocation.minExecutors": 0,
        "spark.dynamicAllocation.initialExecutors": 0,
        "spark.shuffle.service.enabled": 0,
        "spark.sql.shuffle.partitions": 200,
        "spark.sql.autoBroadcastJoinThreshold": 10 * 1024 * 1024
    }

    def __init__(self, args):
        super(SparkAppExtractor, self).__init__(args)
        self.entity_type = 2

    def get_spark_conf(self, entity_id):
        query = "SELECT detail1 FROM blackboards WHERE entity_id='{}' AND entity_type='conf' AND d1='app'".format(
            entity_id)
        return self.db.execute(query)

    def decode_spark_conf(self, rows, app_data):
        for row in rows:
            detail1 = decode_base64_and_gzip_decompress(row[0])
            conf = json.loads(detail1)
            for c in self.REC_CONFS:
                if c in conf:
                    app_data['conf'][c] = conf[c]
        return app_data

    def get_spark_jobs_annotations_from_es(self, entity_id, app_data):
        try:
            search = Search(using=self.es.es, index="app-search").query("term", id=entity_id)
            row = next(search.scan())
            if row:
                row = row.to_dict()
                app_data['startTime'] = row.get('startTime', 0)
                app_data['duration'] = row.get('duration', 0)
                app_data['memorySeconds'] = row.get('memorySeconds', 0)
                app_data['vcoreSeconds'] = row.get('vcoreSeconds', 0)
                if self.with_full_annotation:
                    app_data['annotation'] = {k: v for k, v in list(row.items()) if k not in
                                              ['user', 'queue', 'startTime', 'duration', 'memorySeconds',
                                               'vcoreSeconds']}
        except Exception as e:
            print('ERROR while getting spark job annotation for query: ' + app_data['id'], e)

    def initialize_app_data(self, entity_id, app_data):
        app_data['id'] = entity_id
        app_data['user'] = '-'
        app_data['queue'] = '-'
        app_data['startTime'] = 0
        app_data['duration'] = 0
        app_data['memorySeconds'] = 0
        app_data['vcoreSeconds'] = 0
        app_data['conf'] = {}
        app_data['recommendations'] = []

    def extract_app_data(self, entity_id, app_data):
        self.get_spark_jobs_annotations_from_es(entity_id, app_data)
        rows = self.get_spark_conf(entity_id)
        self.decode_spark_conf(rows, app_data)


class MrAppExtractor(AppExtractor):
    """
    Fetch data from ES & DB for MR apps with recommendations for last N (default 30) days
    The output contains:
    * appId
    * user
    * queue
    * startTime
    * duration
    * memory-seconds
    * vcore-seconds
    * full list of recommendations
    Sample output:
    [
        {
            "id": "job_1631609793721_0257",
            "user": "rohankatimada",
            "startTime": "2021-09-19T10:26:51.589Z",
            "duration": 22000,
            "memorySeconds": 444068,
            "vcoreSeconds": 41,
            "conf": {},
            "recommendations": [
                {
                    "recommendedValue": "506",
                    "currentValue": "12288",
                    "event": "SummaryEvent",
                    "conf": "mapreduce.reduce.memory.mb"
                },
                {
                    "recommendedValue": "-Xmx405m",
                    "currentValue": "-Xmx9830m",
                    "event": "SummaryEvent",
                    "conf": "mapreduce.reduce.java.opts"
                },
                {
                    "recommendedValue": "-Xmx1000m",
                    "currentValue": "-Xmx4915m",
                    "event": "SummaryEvent",
                    "conf": "mapreduce.map.java.opts"
                },
                {
                    "recommendedValue": "1250",
                    "currentValue": "6144",
                    "event": "SummaryEvent",
                    "conf": "mapreduce.map.memory.mb"
                }
            ],
            "generatingQueryType": "hive",
            "queue": "default",
            "insights": [
                {
                    "trm": 506,
                    "srj": 405,
                    "paid": "rohankatimada_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
                    "srm": 506,
                    "cuid": "j-3GD1TNHPQ1EPZ",
                    "amr": 353,
                    "event": "MRTooLargeReduceEvent",
                    "cym": 32,
                    "crm": 12288
                },
                {
                    "cmm": 6144,
                    "amm": 870,
                    "paid": "rohankatimada_20210919102547_7794837c-c231-4829-8e9d-94744cc38e9a",
                    "event": "MRTooLargeMapEvent",
                    "smm": 1250,
                    "smj": 1000,
                    "cuid": "j-3GD1TNHPQ1EPZ",
                    "tmm": 1250,
                    "cym": 32
                }
            ],
            "totalMapSlotDuration": 10051,
            "totalReduceSlotDuration": 3154
        }
    ]
    """

    def __init__(self, args):
        super(MrAppExtractor, self).__init__(args)
        self.entity_type = 0

    def get_mr_jobs_annotations_from_es(self, mr_job_id, app_data):
        try:
            search = Search(using=self.es.es, index="app-search").query("term", id=mr_job_id)
            row = next(search.scan())
            if row:
                row = row.to_dict()
                app_data['startTime'] = row.get('startTime', 0)
                app_data['duration'] = row.get('duration', 0)
                app_data['memorySeconds'] = row.get('memorySeconds', 0)
                app_data['vcoreSeconds'] = row.get('vcoreSeconds', 0)
                app_data['totalMapSlotDuration'] = row.get('totalMapSlotDuration', 0)
                app_data['totalReduceSlotDuration'] = row.get('totalReduceSlotDuration', 0)
                app_data['generatingQueryType'] = row.get('appt', '')
                if self.with_full_annotation:
                    app_data['annotation'] = {k: v for k, v in list(row.items()) if k not in
                                              ['user', 'queue', 'startTime', 'duration', 'memorySeconds',
                                               'vcoreSeconds', 'totalMapSlotDuration', 'totalReduceSlotDuration',
                                               'appt']}
        except Exception as e:
            print('ERROR while getting mr job annotation for query: ' + app_data['id'], e)

    def initialize_app_data(self, entity_id, app_data):
        app_data['id'] = entity_id
        app_data['user'] = '-'
        app_data['startTime'] = 0
        app_data['duration'] = 0
        app_data['memorySeconds'] = 0
        app_data['vcoreSeconds'] = 0
        app_data['conf'] = {}
        app_data['recommendations'] = []
        app_data['generatingQueryType'] = ''

    def extract_app_data(self, entity_id, app_data):
        self.get_mr_jobs_annotations_from_es(entity_id, app_data)


APP_EXTRACTOR_MAP = {
    'hive': HiveAppExtractor,
    'hive-on-tez': HiveOnTezAppExtractor,
    'spark': SparkAppExtractor,
    'mr': MrAppExtractor
}


def print_error_and_exit(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
    sys.exit(1)


def debug(msg):
    if VERBOSE:
        print(msg)


def get_command_output(cmd, env):
    out = None
    run_env = os.environ.copy().update(env)
    cmdRun = ' '.join(cmd)
    try:
        debug("Running command: {}, env={}\n".format(cmdRun, env))
        p = subprocess.Popen(cmdRun, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=run_env)
        out, err = p.communicate()
        # out = subprocess.check_output(cmdRun, stderr=subprocess.STDOUT, shell=True)
        if p.returncode != 0:
            sys.stderr.write(err + '\n')
            print_error_and_exit("Error while running command: {}".format(cmdRun))
    except OSError as e:
        print_error_and_exit("OSError errno: {} error: {} filename: {}".format(e.errno, e.strerror, e.filename))
    except Exception as e:
        traceback.print_exc()
        print_error_and_exit('Exception while running command: {}'.format(' '.join(cmd)))

    return out


def decode_base64_and_gzip_decompress(text):
    try:
        decoded = IO()
        decoded.write(base64.b64decode(text))

        # seek at beginning of buffer
        decoded.seek(0)
        uncompressed_buffer = gzip.GzipFile(fileobj=decoded, mode="rb")
        uncompressed_string = uncompressed_buffer.read()
        uncompressed_buffer.close()

        return uncompressed_string
    except Exception as e:
        return text


def main():
    parser = argparse.ArgumentParser(description='Extract interesting hive apps')
    parser.add_argument('--out-extract-file', default='rec_apps.json',
                        help='Output file name to write data for apps with recommendations (from the `extract` stage).')
    parser.add_argument('--out-score-file', default='rec_apps.csv',
                        help='Output file name to write data for interesting apps including scores (from the `score` '
                             'stage).')
    parser.add_argument('--overwrite',
                        help='Overwrite the output file specified by --out-file. Default without this option is to '
                             'abort if --out-file already exists',
                        action='store_true')
    parser.add_argument('--app-type', default=None, type=str,
                        help='One of: {}'.format(', '.join(list(APP_EXTRACTOR_MAP.keys()))))
    parser.add_argument('--days', type=int, default=DEFAULT_NUM_DAYS,
                        help='Get apps within specified number of days in the past from --end-date. Default {} days. '.format(
                            DEFAULT_NUM_DAYS) +
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
    parser.add_argument('--max-apps', type=int, default=1000000,
                        help='Maximum number of applications to process. Default 1000000 (1M). Set to -1 for '
                             'unlimited. ' +
                             'If specified, a maximum of --max-apps most recent applications are processed.')
    parser.add_argument('--sort-key', default=DEFAULT_SORT_KEY,
                        help='Sort the output based on the given key. Default: {}'.format(DEFAULT_SORT_KEY))
    parser.add_argument('--sort-order', default=DEFAULT_SORT_ORDER,
                        help='Sort order for the output (desc or asc). Default: {}'.format(DEFAULT_SORT_ORDER))
    parser.add_argument('--with-insights', help='Fetch all insights generated for the applications. '
                                                'Default behavior without the flag: do not fetch insights.',
                        action='store_true')
    parser.add_argument('--with-request-user',
                        help='Populate the output with actual user who executed the Hive query in the requestUser '
                             'attribute. Default behavior without the flag: do not populate requestUser with'
                             ' real Hive user.'
                             'Note: Using this option may significantly slow down the script. This flag is only '
                             'meaningful with Hive on MR with Sentry for now.',
                        action='store_true')
    parser.add_argument('--with-full-annotation',
                        help='Populate the output with full annotation data for each app. This increases the size of '
                             'the output file, but retrieves more contextual data as a basis to make better tuning'
                             ' recommendations.',
                        action='store_true')
    parser.add_argument('--stages', type=str, default='extract',
                        help='Stages to perform. The default is to perform the extract stage only. ' +
                             'You can specify `extract` to only perform extraction of apps with recommendations ('
                             'default), '
                             '`score` to only perform filtering and scoring of apps with recommendations, or ' +
                             '`all` to perform both. If using `score`, --out-extract-file is used as the input to '
                             'perform filtering and scoring.')
    parser.add_argument('--min-duration-seconds', type=int, default=180,
                        help='Any apps whose duration in seconds is shorter than --min-duration-seconds will be '
                             'filtered out from the final result.')
    parser.add_argument('--min-vcore-seconds', type=int, default=10000,
                        help='Any apps that have vcore seconds shorter than --min-vcore-seconds AND memory seconds '
                             'shorter than --min-memory-seconds will be filtered out.')
    parser.add_argument('--min-memory-seconds', type=int, default=1000000,
                        help='Any apps that have vcore seconds shorter than --min-vcore-seconds AND memory seconds '
                             'shorter than --min-memory-seconds will be filtered out.')
    parser.add_argument('--score-algo', type=str, default='ale',
                        help='Scoring algorithm.  Default: ale')
    parser.add_argument('--score-params', type=str, default='8,12,22,28,30',
                        help='Parameters for the scoring function.')
    parser.add_argument('--print-memory-usage', help='Print memory consumption as apps are being extracted',
                        action='store_true')
    parser.add_argument('--cluster-uid', default="default", type=str,
                        help='Apps will be retrieved for the cluster specified by this UID. Defaults to the "default" '
                             'cluster')
    parser.add_argument('--verbose', help='Print verbose messages on stdout', action='store_true')

    args = parser.parse_args()

    if args.app_type is None or args.app_type not in APP_EXTRACTOR_MAP:
        print_error_and_exit('--app-type must be one of the following: {}'.format(', '.join(list(APP_EXTRACTOR_MAP.keys()))))

    if args.app_type != 'hive' and args.with_request_user:
        print(
            '[warning] --with-request-user can only be used with --app-type hive. Automatically disabling '
            '--with-request-user')
        args.with_request_user = False

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

    if not args.overwrite and os.path.exists(args.out_extract_file) and args.stages != 'score':
        print_error_and_exit(
            '{} already exists. Specify a different --out-extract-file or use --overwrite option.'.format(
                args.out_extract_file))

    if not args.overwrite and os.path.exists(args.out_score_file) and args.stages != 'extract':
        print_error_and_exit(
            '{} already exists. Specify a different --out-score-file or use --overwrite option.'.format(
                args.out_score_file))

    base_dir = os.path.dirname(os.path.abspath(args.out_extract_file))
    if not os.path.isdir(base_dir):
        print_error_and_exit('The output directory {} is not a directory or does not exist.'.format(base_dir))

    base_dir = os.path.dirname(os.path.abspath(args.out_score_file))
    if not os.path.isdir(base_dir):
        print_error_and_exit('The output directory {} is not a directory or does not exist.'.format(base_dir))

    global VERBOSE
    VERBOSE = args.verbose

    extractor = APP_EXTRACTOR_MAP[args.app_type](args)
    extractor.run()


if __name__ == "__main__":
    main()