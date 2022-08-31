import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
import math
import argparse
import sys
import time
from pyunravel import ES, DB
import ast
import json
from datetime import datetime, timedelta, date
import pytz
import config


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
        if response[0][0].encode("utf-8") == "SUCCESS":
            return response[0][1].encode("utf-8")
        elif response[0][0].encode("utf-8") == "FAILURE":
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
            # filter_key_split = self.filter_keys.split('.*')
            # filter_key = filter_key[0]
            for host in hosts:
                if self.filter_keys not in host['roles']:
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
            new_calculated_df['Free'] = '{} {}'.format((value[0] - value[1]), unit)
            free = free + (value[0] - value[1])
            new_calculated_df_list.append(new_calculated_df)
        can_be_saved_by = (free / len(usage_map))
        new_calculated_df = {}
        new_calculated_df['Host'] = "Total Free {} across selected nodes".format(usage)
        new_calculated_df['Peak {} utilization'.format(usage)] = ""
        new_calculated_df['Free'] = '({})/{} = {} {}'.format(free,
                                                             len(usage_map),
                                                             can_be_saved_by, unit)
        new_calculated_df_list.append(new_calculated_df)
        df = pd.DataFrame(new_calculated_df_list)
        df = df[['Host', 'Peak {} utilization'.format(usage), 'Free']]
        if usage == 'Memory':
            string_to_display = "You can save around {} {} {} across selected nodes".format(can_be_saved_by, unit,
                                                                                            usage)
        else:
            string_to_display = "You can save around {} {} of CPU across selected nodes".format(can_be_saved_by, unit)
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
                             filename='/opt/unravel/data/apps/unity-one/src/assets/reports/jobs/node_memory.html')
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
                             filename='/opt/unravel/data/apps/unity-one/src/assets/reports/jobs/node_cpu.html')

    def generate(self):
        toolbar_width = 40
        print("Node Reduction report started!!!!")
        sys.stdout.write("[%s]" % (" " * toolbar_width))
        sys.stdout.flush()
        sys.stdout.write("\b" * (toolbar_width + 1))  # return to start of line, after '['
        # cluster_disc_entity_id = self.generate_cluster_discovery_report()
        # print(cluster_disc_entity_id)
        cluster_disc_resp = self.get_report_payload('139650209604298')
        hosts = self.filter_hosts_based_on_keys_by_user(cluster_disc_resp)
        memory_df_dict, cpu_df_dict = self.fetch_cpu_and_memory_per_host_ts(hosts, cluster_disc_resp)
        self.plot_cpu_and_memory_figures(memory_df_dict, cpu_df_dict)
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