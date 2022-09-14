import json
import logging
import sys
import requests
import config
import plotly.express as px
import plotly.graph_objects as go
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pyunravel import ES, DB
from elasticsearch6 import Elasticsearch
import pandas as pd
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc

from config import *
from pylib import apps, pdutil, ioutil, dashutil, uprops, esutil, report_utils

logger = logging.getLogger('unravel')
es_url = es_url
unravel_url = unravel_url


class Report:
    def __init__(self, dest, filter_keys,
                 buffer=20, days=None, start_date=None, end_date=None, mwatch=None):
        self.filter_keys = filter_keys
        self.dest = dest
        self.buffer_value = buffer / 100.0
        self.days = days
        if start_date is not None:
            self.start_time = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            self.end_time = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            self.end_time = datetime.now(tz=timezone.utc)
            self.start_time = self.end_time - timedelta(days=self.days)
        self.db = DB("jdbc:postgresql://127.0.0.1:4339/unravel", username="unravel",
                     password="ve9NEEyDj7ArewVl5hyu5W3kDja8xuMUOVTSEpjJFoKNnw6Z1GpyA88mJOn1QAwI")

    def update_progress_bar(self):
        sys.stdout.write("###")
        sys.stdout.flush()

    def figures_to_html(self, figs,
                        filename=None):
        with open(filename, 'w') as dashboard:
            dashboard.write("<html><head></head><body>" + "\n")
            for fig in figs:
                inner_html = fig.to_html(include_plotlyjs="cdn").split('<body>')[1].split('</body>')[0]
                dashboard.write(inner_html)
            dashboard.write("</body></html>" + "\n")
        print("file {} generated succefully.......".format(filename))

    def generate_table_fig(self, df, title):
        self.update_progress_bar()
        fig = go.Figure(data=[go.Table(
            header=dict(values=list(df.columns),
                        fill_color='paleturquoise',
                        align='left'),
            cells=dict(values=df.transpose().values.tolist(),
                       fill_color='lavender',
                       align='left'))
        ])
        fig.update_layout(title_text=title, title_x=0.5)
        fig.update_layout(height=500)
        return fig

    def check_if_task_is_completed(self, task_id):
        self.update_progress_bar()
        query = "SELECT task_status, entity_id FROM ondemand_tasks WHERE task_id = '{}'".format(task_id)
        response = self.db.execute(query)
        print(response)
        if response[0][0] == "SUCCESS":
            return response[0][1]
        elif response[0][0] == "FAILURE":
            return False
        else:
            time.sleep(2)
            return self.check_if_task_is_completed(task_id)

    def get_report_payload(self, entity_id):
        query = "SELECT output_json FROM report_instances WHERE report_instance_id = '{}'".format(entity_id)
        response = self.db.execute(query)
        return response[0][0]

    def generate_cluster_discovery_report(self):

        headers = {
            'Accept': 'application/json, text/plain, */*',
        }
        print(int(self.start_time.strftime("%s")) * 1000)
        params = {
            'start': int(self.start_time.strftime("%s")) * 1000,
            'end': int(self.end_time.strftime("%s")) * 1000,
        }

        response = requests.get('http://localhost:5001/cluster-discovery', params=params, headers=headers)
        if response.status_code == 200:
            task_id = response.json()['task_id']
            time.sleep(5)
            status_entity_id = self.check_if_task_is_completed(task_id)
            if status_entity_id == False:
                sys.stderr.write("TASK {} failed!!!".format(task_id) + "\n")
                sys.stderr.flush()
                sys.exit(1)
            else:
                return status_entity_id
        else:
            sys.stderr.write("cluster discovery report failed" + "\n")
            sys.stderr.flush()
            sys.exit(1)

    def get_percentage_reduction_on_buffer(self, value):
        return (value - (value * self.buffer_value))

    def fetch_cpu_and_memory_per_host_ts(self, host_names, cluster_discovery_response):
        memory_df_dict = {}
        cpu_df_dict = {}
        cluster_discovery_response = json.dumps(cluster_discovery_response)
        cluster_discovery_response = eval(cluster_discovery_response)
        cluster_discovery_response = json.loads(cluster_discovery_response)
        host_level_metrics = cluster_discovery_response['host_level_metrics']
        for key, value in host_level_metrics[0]['CPU'].items():
            if key in host_names:
                cpu_df = pd.DataFrame(value['metrics'])
                cpu_df.columns = cpu_df.columns = ['timeseries', 'actual', 'used']
                cpu_df['timeseries'] = pd.to_datetime(cpu_df['timeseries'], origin='unix', unit='ms')
                cpu_df_dict[key] = cpu_df
        for key, value in host_level_metrics[1]['RAM'].items():
            if key in host_names:
                memory_df = pd.DataFrame(value['metrics'])
                memory_df.columns = memory_df.columns = ['timeseries', 'actual', 'used']
                memory_df['timeseries'] = pd.to_datetime(memory_df['timeseries'], origin='unix', unit='ms')
                memory_df_dict[key] = memory_df
        return memory_df_dict, cpu_df_dict

    def filter_hosts_based_on_keys_by_user(self, payload):
        payload = json.dumps(payload)
        payload = eval(payload)
        payload = json.loads(payload)
        host_list = []
        hosts = payload['hosts']
        if '*' in self.filter_keys:
            filter_key_split = self.filter_keys.split('.*')
            filter_key = filter_key_split[0]
            for host in hosts:
                if filter_key not in host['roles']:
                    host_list.append(host['instance_id'])
        else:
            for host in hosts:
                if len(host['roles']) > 1:
                    host_list.append(host['instance_id'])
                elif self.filter_keys not in host['roles']:
                    host_list.append(host['instance_id'])
        return host_list

    def calculate_node_save_metrics_and_plot_table(self, usage_map, usage=False):
        free = 0
        new_calculated_df_list = []
        if usage == 'Memory':
            unit = 'GB'
        else:
            unit = 'Cores'
        for key, value in usage_map.items():
            new_calculated_df = {}
            new_calculated_df['Host'] = key
            new_calculated_df['Peak {} utilization'.format(usage)] = '{} {}'.format(value[1], unit)
            new_calculated_df['Free'] = '{:.2f} {}'.format((value[0] - value[1]), unit)
            free = free + (value[0] - value[1])
            new_calculated_df_list.append(new_calculated_df)
        can_be_saved_by = (free / len(usage_map))
        new_calculated_df = {}
        new_calculated_df['Host'] = "Total Free {} across selected nodes".format(usage)
        new_calculated_df['Peak {} utilization'.format(usage)] = ""
        new_calculated_df['Free'] = '({:.2f})/{} = {:.2f} {}'.format(free,
                                                                     len(usage_map),
                                                                     can_be_saved_by, unit)

        new_calculated_df_list.append(new_calculated_df)
        new_calculated_df = {}
        new_calculated_df['Host'] = "Total Free {} after removing buffer across selected nodes".format(usage)
        new_calculated_df['Peak {} utilization'.format(usage)] = ""
        new_calculated_df['Free'] = "{:.2f} {} - {:.2f} {} = {:.2f} {}".format(can_be_saved_by, unit,
                                                                               (can_be_saved_by * self.buffer_value),
                                                                               unit,
                                                                               self.get_percentage_reduction_on_buffer(
                                                                                   can_be_saved_by), unit)
        new_calculated_df_list.append(new_calculated_df)
        df = pd.DataFrame(new_calculated_df_list)
        df = df[['Host', 'Peak {} utilization'.format(usage), 'Free']]
        if usage == 'Memory':
            string_to_display = "You can save around {:.2f} {} {} across selected nodes".format(
                self.get_percentage_reduction_on_buffer(can_be_saved_by), unit, usage)
        else:
            string_to_display = "You can save around {:.2f} {} of CPU across selected nodes".format(
                self.get_percentage_reduction_on_buffer(can_be_saved_by), unit)
        fig = self.generate_table_fig(df, string_to_display)
        return fig

    def plot_cpu_and_memory_figures(self, memory_df_dict, cpu_df_dict):
        memory_fig_list = []
        cpu_fig_list = []
        memory_usage_map = {}
        cpu_usage_map = {}
        mem_value_list = []
        cpu_value_list = []
        for df_key, df_value in memory_df_dict.items():
            fig = px.line(df_value, y=['used', 'actual'], x="timeseries", height=400,
                          title='Memory Actual/Usage time series for {}'.format(df_key))
            peak_memory = df_value["used"].max()
            fig.add_annotation(x=df_value.iloc[df_value["used"].idxmax()]["timeseries"],
                               y=peak_memory,
                               text="Peak {} Memory".format(peak_memory))
            memory_fig_list.append(fig)
            mem_value_list = [df_value['actual'].mean(), peak_memory]
            memory_usage_map[df_key] = mem_value_list
        f = self.calculate_node_save_metrics_and_plot_table(memory_usage_map, 'Memory')
        memory_fig_list.insert(0, f)
        self.figures_to_html(memory_fig_list,
                             filename=f'{self.dest}/node_memory.html')
        for df_key, df_value in cpu_df_dict.items():
            fig = px.line(df_value, y=['used', 'actual'], x="timeseries", height=400,
                          title='CPU Usage percentage time series for {}'.format(df_key))
            peak_cores = df_value["used"].max()
            fig.add_annotation(x=df_value.iloc[df_value["used"].idxmax()]["timeseries"],
                               y=peak_cores,
                               text="Peak {} Cores".format(peak_cores))
            cpu_fig_list.append(fig)
            cpu_value_list = [df_value['actual'].mean(), peak_cores]
            cpu_usage_map[df_key] = cpu_value_list
        f = self.calculate_node_save_metrics_and_plot_table(cpu_usage_map, 'Cores')
        cpu_fig_list.insert(0, f)
        self.figures_to_html(cpu_fig_list,
                             filename=f'{self.dest}/cpu_memory.html')

    def generate(self):
        cluster_disc_entity_id = self.generate_cluster_discovery_report()
        # print(cluster_disc_entity_id)
        cluster_disc_resp = self.get_report_payload(cluster_disc_entity_id)
        hosts = self.filter_hosts_based_on_keys_by_user(cluster_disc_resp)
        if len(hosts) == 0:
            return
        memory_df_dict, cpu_df_dict = self.fetch_cpu_and_memory_per_host_ts(hosts, cluster_disc_resp)
        self.plot_cpu_and_memory_figures(memory_df_dict, cpu_df_dict)
        print("Done and Dusted!!!!!")


def layout(params):
    es = Elasticsearch(es_url, http_auth=(es_username, es_password) if es_username else None)
    elastic_search = esutil.ES()
    query = {
        "aggs": {"min_start_time": {"min": {"field": "startTime"}}, "max_start_time": {"max": {"field": "startTime"}}}}
    aggs = elastic_search.query_es(path='/feature*/_search?size=0', query=query)
    min_start_time = aggs['aggregations']['min_start_time']['value_as_string'] if 'aggregations' in aggs else 0
    max_start_time = aggs['aggregations']['max_start_time']['value_as_string'] if 'aggregations' in aggs else 0
    feature_info = f"Feature generation completed for {min_start_time} to {max_start_time}" if min_start_time is not 0 else 'Feature not available'
    tag_values_pair = report_utils.get_tags_values()
    tags_all = list(tag_values_pair.keys())

    return [
        dbc.FormGroup([
            dbc.Label('Look Back'),
            dbc.InputGroup(
                [
                    dbc.Input(
                        id='days', type='number', min=0, step=1,
                        value=params['days'] if (params is not None and 'days' in params) else None
                    ),
                    dbc.InputGroupAddon("days", addon_type="append"),
                    dbc.Tooltip(
                        'The period of time over which applications are selected for report generation',
                        target='days',
                        placement='bottom-end',
                    ),
                ], size="sm",
            ),
        ]),
        dbc.FormGroup([
            dbc.Checklist(id='date-time-picker', options=[{'label': 'Use Exact Date-Time', 'value': 'DT'}],
                          value=['DT'] if (params is not None and params['start_date'] is not None) else []),
            html.Div(id='date-time')
        ]),
        dbc.FormGroup([
            dbc.Label('filter_keys'),
            dbc.Input(id='filter_keys', value=params['filter_keys'] if params else None),
            dbc.Tooltip(
                'nodes will be filtered based on these roles',
                target='filter_keys',
                placement='bottom-end',
            )]),
        dbc.FormGroup([
            dbc.Label('buffer'),
            dbc.Input(id='buffer', type="number", min=1, step=1, value=params['buffer'] if params else None),
            dbc.Tooltip(
                'buffer helps to reduce the value',
                target='buffer',
                placement='bottom-end',
            )])
    ]


def verify_params(params):
    json_errors_list = []
    if params['start_date'] is None and params['end_date'] is None and params['days'] is None:
        json_errors_list.append('params should must have start_date and end_date or days')
    if 'days' in params and params['days'] is not None and not isinstance(params['days'], int):
        json_errors_list.append('days in params should must be a number')
    if params['buffer'] is None or not isinstance(params['buffer'], int):
        json_errors_list.append('params should must contain a buffer which should be a number')

    return json_errors_list


def params_from_layout(children):
    params = {}

    start_date = dashutil.prop(children, 'date-picker', 'startDate')
    end_date = dashutil.prop(children, 'date-picker', 'endDate')

    params['start_date'] = start_date
    params['end_date'] = end_date

    days = dashutil.prop(children, 'days', 'value')
    if days is None and start_date is None:
        raise Exception('days/date-time range is not specified')
    if days is not None:
        try:
            days = int(days)
        except:
            raise Exception('days must be number')
        if days <= 0:
            raise Exception('days must be positive number')
    params['days'] = days

    filter_keys = dashutil.prop(children, 'filter_keys', 'value')
    if filter_keys is None:
        raise Exception('filter_keys is not specified')
    params['filter_keys'] = filter_keys

    buffer = dashutil.prop(children, 'buffer', 'value')
    if buffer is None:
        raise Exception('buffer is not specified')
    params['buffer'] = buffer

    return params


if __name__ == '__main__':
    import argparse

    logger.setLevel(logging.DEBUG)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s |  %(message)s')

    parser = argparse.ArgumentParser(description='generates kip trend reports')
    parser.add_argument('--dest', help='destination directory', required=True)
    parser.add_argument('--days', help='number of days to look back', required=True, type=int)
    parser.add_argument('--start_date', help='start time')
    parser.add_argument('--end_date', help='end time')
    parser.add_argument('--filter_key', help='nodes will be filtered based on these roles.')
    parser.add_argument('--buffer', help='number of apps in report, default 20', type=int, default=20)
    args = parser.parse_args()

    Report(**vars(args)).generate()
