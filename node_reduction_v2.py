import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
import math
import argparse
import sys
import time

from plotly.subplots import make_subplots
from pyunravel import ES, DB
import ast
import json
from datetime import datetime, timedelta, date
import pytz
# import config
# from config import *
#from unravel.tools.crypto.password import PasswordEncryptor


# pe_conf = {"AES": {"key": unravel_tools_password_encryptor_aes_key}}
# PasswordEncryptor.configure_default(pe_conf)

class NodeReduction:
    def __init__(self, filter_keys, buffer):
        self.filter_keys = filter_keys
        self.buffer = buffer
        self.days = 30
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=self.days)
        self.end_time = datetime.now(tz=pytz.utc)
        self.start_time = self.end_time - timedelta(days=self.days)
        # self.db = DB.from_unravel_properties(props_path="/opt/unravel/data/conf/unravel.properties")
        self.db = DB("jdbc:postgresql://127.0.0.1:4339/unravel", username="unravel",
                     password="ve9NEEyDj7ArewVl5hyu5W3kDja8xuMUOVTSEpjJFoKNnw6Z1GpyA88mJOn1QAwI")

    def update_progress_bar(self):
        sys.stdout.write("###")
        sys.stdout.flush()

    def figures_to_html(self, figs,
                        filename="/opt/unravel/data/apps/unity-one/src/assets/reports/jobs/node_reduction.html"):
        with open(filename, 'w') as dashboard:
            dashboard.write("<html><head></head><body>" + "\n")
            for fig in figs:
                inner_html = fig.to_html(include_plotlyjs="cdn").split('<body>')[1].split('</body>')[0]
                dashboard.write(inner_html)
            dashboard.write("</body></html>" + "\n")
        print("file {} generated succefully.......".format(filename))

    def convert_size(self, size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

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

    def create_px_for_hosts(self, df):
        print(df.to_string())
        fig2 = px.line(df, y=['memory_used', 'spec_Memory'], x="Host", height=400,
                       title='Spec Memory/Usage Memory trend for selected hosts')
        fig = px.line(df, y=['cores_used', 'spec_cores'], x="Host", height=400,
                      title='Spec Cores/Usage Cores trend for selected hosts')
        fig3 = px.line(df, y=['disk_used', 'spec_disk'], x="Host", height=400,
                       title='Spec Disk/Usage Disk trend for selected hosts')
        return fig2, fig, fig3


    def check_if_task_is_completed(self, task_id):
        self.update_progress_bar()
        query = "SELECT task_status, entity_id FROM ondemand_tasks WHERE task_id = '{}'".format(task_id)
        response = self.db.execute(query)
        if response[0][0].encode("utf-8") == "SUCCESS":
            return response[0][1].encode("utf-8")
        elif response[0][0].encode("utf-8") == "FAILURE":
            return False
        else:
            time.sleep(10)
            return self.check_if_task_is_completed(task_id)

    def generate_cluster_mapping_per_host_report(self):
        self.update_progress_bar()
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            # Already added when you pass json= but not when you pass data=
            # 'Content-Type': 'application/json',
        }

        json_data = {
            'instance_type': [
                'x1.32xlarge',
                'x1e.32xlarge',
            ],
            'cloud_provider': 'EC2',
            'custom_percentile': None,
            'prefs': {
                'region': 'AWS GovCloud (US-East)',
                'additional_storage_type': {
                    'name': 's3',
                    'pretty_name': 'S3',
                    'type': 'object_storage',
                    'id': 'object_storage',
                    'text': 'Object storage',
                },
                'migration_type': 'mappings_per_host',
                'selected': {
                    'x1.32xlarge': {
                        'override_cost': 16.006,
                    },
                    'x1e.32xlarge': {
                        'override_cost': 32,
                    },
                },
            },
        }

        response = requests.post('http://localhost:5001/cloud-mappings-reports', headers=headers, json=json_data)
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
            sys.stderr.write("cloud-mappings-reports failed" + "\n")
            sys.stderr.flush()
            sys.exit(1)

    def get_report_payload(self, entity_id):
        query = "SELECT output_json FROM report_instances WHERE report_instance_id = '{}'".format(entity_id)
        response = self.db.execute(query)
        return response[0][0]

    def create_host_fig(self, payload_unicode, key):
        self.update_progress_bar()
        payload = json.dumps(payload_unicode)
        hosts = []
        payload = eval(payload)
        payload = json.loads(payload)
        for hosts_data in payload[key]['hosts']:
            if self.filter_keys not in hosts_data['usage']['roles']:
                hosts.append(hosts_data['id'])
        return hosts

    def fetch_cpu_and_memory_per_host_ts(self, host_names):
        memory_df_dict = {}
        cpu_df_dict = {}
        cloudera_api_url = "http://sd11.unraveldata.com:7180"
        for host in host_names:
            cpu_list = []
            memory_list = []
            act_mem_list = []
            api = '{}/api/v33/timeseries?query=select+cpu_percent+where+hostname={}&from={}&to={}&desiredRollup=HOURLY'.format(
                cloudera_api_url, host, self.start_date, self.end_date)
            response = requests.get(api, auth=('admin', 'admin'))
            # print(response.status_code)
            if response.status_code == 200:
                # print(response.json()['items'][0]['timeSeries'][0]['data'])
                for data in response.json()['items'][0]['timeSeries'][0]['data']:
                    if len(data) != 0:
                        cpu_dict = {}
                        cpu_dict['timestamp'] = data['timestamp']
                        cpu_dict['percentage'] = data['value']
                        cpu_list.append(cpu_dict)
            api = '{}/api/v33/timeseries?query=select+physical_memory_used,physical_memory_total+where+hostname={}&from={}&to={}&desiredRollup=HOURLY'.format(
                cloudera_api_url, host, self.start_date, self.end_date)
            response = requests.get(api, auth=('admin', 'admin'))
            if response.status_code == 200:
                for data in response.json()['items'][0]['timeSeries'][0]['data']:
                    if len(data) != 0:
                        memory_dict = {}
                        memory_dict['timestamp'] = data['timestamp']
                        memory_dict['value'] = data['value']
                        memory_list.append(memory_dict)
                for data in response.json()['items'][0]['timeSeries'][1]['data']:
                    if len(data) != 0:
                        memory_dict = {}
                        memory_dict['timestamp'] = data['timestamp']
                        memory_dict['actual'] = data['value']
                        act_mem_list.append(memory_dict)

            if len(cpu_list) != 0:
                cpu_df = pd.DataFrame(cpu_list)
                cpu_df_dict[host] = cpu_df
            if len(memory_list) != 0 and len(act_mem_list) != 0:
                memory_df = pd.DataFrame(memory_list)
                df = pd.DataFrame(act_mem_list)
                memory_df['actual'] = df['actual'].values
                memory_df_dict[host] = memory_df
        return memory_df_dict, cpu_df_dict


    def calculate_node_save_metrics_and_plot_table(self, usage_map, type):
        if type == 'CPU':
            cpu_free = 0
            new_calculated_df_list = []

            for key, value in usage_map.items():
                new_calculated_df = {}
                new_calculated_df['Host'] = key
                new_calculated_df['Peak utilization'] = '{} %'.format(round(value))
                new_calculated_df['Free'] = '{} %'.format(round((100 - value)))
                cpu_free = cpu_free + round((100 - value))
                new_calculated_df_list.append(new_calculated_df)
            cpu_can_be_saved_by = round(((cpu_free - self.buffer) / len(usage_map)))
            new_calculated_df = {}
            new_calculated_df['Host'] = "Total Free CPU Percentage"
            new_calculated_df['Peak utilization'] = ""
            new_calculated_df['Free'] = '({}% - {}%)/{} = {}%'.format(cpu_free,self.buffer,len(usage_map),cpu_can_be_saved_by)
            new_calculated_df_list.append(new_calculated_df)
            df = pd.DataFrame(new_calculated_df_list)
            string_to_display = "You can save around {} % CPU resources".format(cpu_can_be_saved_by)
            fig = self.generate_table_fig(df, string_to_display)
        else:
            memory_free = 0
            new_calculated_df_list = []

            for key, value in usage_map.items():
                new_calculated_df = {}
                print((value[0] - value[1]))
                new_calculated_df['Host'] = key
                new_calculated_df['Peak Memory utilization'] = '{}'.format(self.convert_size(value[1]))
                new_calculated_df['Free'] = '{}'.format(self.convert_size((value[0] - value[1])))
                memory_free = memory_free + round((value[0] - value[1]))
                new_calculated_df_list.append(new_calculated_df)
            cpu_can_be_saved_by = round((memory_free / len(usage_map)))
            new_calculated_df = {}
            new_calculated_df['Host'] = "Total Free Memory across selected nodes"
            new_calculated_df['Peak Memory utilization'] = ""
            new_calculated_df['Free'] = '({})/{} = {}'.format(self.convert_size(memory_free),
                                                                      len(usage_map),
                                                                      self.convert_size(cpu_can_be_saved_by))
            new_calculated_df_list.append(new_calculated_df)
            df = pd.DataFrame(new_calculated_df_list)
            string_to_display = "You can save around {}  Memory across selected nodes".format(self.convert_size(cpu_can_be_saved_by))
            fig = self.generate_table_fig(df, string_to_display)
        return fig

    def plot_cpu_and_memory_figures(self, memory_df_dict, cpu_df_dict):
        memory_fig_list = []
        cpu_fig_list = []
        cpu_usage_percentage_map = {}
        memory_usage_map = {}
        for df_key, df_value in memory_df_dict.items():
            fig = px.line(df_value, y=['value', 'actual'], x="timestamp", height=400,
                          title='Memory Actual/Usage time series for {}'.format(df_key))
            m1 = df_value["value"].max()
            fig.add_annotation(x=df_value.iloc[df_value["value"].idxmax()]["timestamp"],
                               y=m1, row=1, col=1,
                               text="Peak {}".format(self.convert_size(m1)))
            memory_fig_list.append(fig)
            mem_value_list = [df_value['actual'].mean(),m1]
            memory_usage_map[df_key] = mem_value_list
        f = self.calculate_node_save_metrics_and_plot_table(memory_usage_map, type='MEMORY')
        memory_fig_list.insert(0, f)
        self.figures_to_html(memory_fig_list,
                             filename='node_memory2.html')
        list_dict = [k for k, v in cpu_df_dict.items()]
        print(list_dict)
        check = lambda x: 1 if x % 2 == 0 else 0
        if not check(len(list_dict)):
            list_dict.append("ExtraHost")
        for key_1, key_2 in (list_dict[i:i + 2] for i in range(0, len(list_dict), 2)):
            if key_2 == 'ExtraHost':
                df_value = cpu_df_dict[key_1]
                fig = go.Figure(data=go.Scatter(x=df_value["timestamp"], y=df_value['percentage'], mode='lines'),
                                layout_yaxis_range=[0, 100])
                fig.update_layout(
                    height=400,
                    title_text='CPU Usage percentage time series for {}'.format(key_1), showlegend=False)
                m = df_value["percentage"].max()
                fig.add_annotation(x=df_value.iloc[df_value["percentage"].idxmax()]["timestamp"],
                                   y=m,
                                   text="Peak {} %".format(m.round(0)))
                cpu_usage_percentage_map[key_1] = m1

            else:
                fig = make_subplots(rows=1, cols=2, subplot_titles=('CPU Usage percentage time series for {}'.format(key_1), 'CPU Usage percentage time series for {}'.format(key_2)))
                df_2_value = cpu_df_dict[key_2]
                df_1_value = cpu_df_dict[key_1]
                fig.add_trace(
                    go.Scatter(x=df_1_value["timestamp"], y=df_1_value['percentage']),
                    row=1, col=1
                )
                m1 = df_1_value["percentage"].max()


                fig.add_trace(
                    go.Scatter(x=df_2_value["timestamp"], y=df_2_value['percentage']),
                    row=1, col=2
                )
                m2 = df_2_value["percentage"].max()

                fig.add_annotation(x=df_1_value.iloc[df_1_value["percentage"].idxmax()]["timestamp"],
                                   y=m1,row=1,col=1,
                                   text="Peak {} %".format(m1.round(0)))
                fig.add_annotation(x=df_2_value.iloc[df_2_value["percentage"].idxmax()]["timestamp"],
                                   y=m2,row=1,col=2,
                                   text="Peak {} %".format(m2.round(0)))

                fig.update_layout(height=400, showlegend=False,yaxis = dict(range=[0, 100]))
                cpu_usage_percentage_map[key_1] = m1
                cpu_usage_percentage_map[key_2] = m2

            cpu_fig_list.append(fig)
        f = self.calculate_node_save_metrics_and_plot_table(cpu_usage_percentage_map, type='CPU')
        cpu_fig_list.insert(0, f)
        self.figures_to_html(cpu_fig_list,
                             filename='node_cpu.html')

    def generate(self):
        toolbar_width = 40
        print("Node Reduction report started!!!!")
        sys.stdout.write("[%s]" % (" " * toolbar_width))
        sys.stdout.flush()
        sys.stdout.write("\b" * (toolbar_width + 1))  # return to start of line, after '['
        key = ""
        fig_list = []
        # entity_id = self.generate_cluster_mapping_per_host_report()
        # payload = self.get_report_payload(entity_id)
        # hosts = self.create_host_fig(payload, key)
        memory_df_dict, cpu_df_dict = self.fetch_cpu_and_memory_per_host_ts(['sd11.unraveldata.com', 'sd02.unraveldata.com', 'sd06.unraveldata.com'])
        self.plot_cpu_and_memory_figures(memory_df_dict, cpu_df_dict)
        # self.figures_to_html(fig_list)
        sys.stdout.write("]\n")
        print("Done and Dusted!!!!!")


def print_error_and_exit(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
    sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Node reduction')
    parser.add_argument('--filter-keys', default=None, type=str,
                        help='Output file name to write data for interesting apps including scores (from the `score` '
                             'stage).')
    parser.add_argument('--buffer', default=None, type=int,
                        help='Should be between 1 to 100')

    args = parser.parse_args()
    print(args.buffer)

    NodeReduction(**vars(args)).generate()