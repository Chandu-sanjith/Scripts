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

class NodeReduction:
    def __init__(self, filter_keys, percentile):
        self.filter_keys = filter_keys
        self.percentile = percentile
        self.db = DB.from_unravel_properties()

    def update_progress_bar(self):
        sys.stdout.write("---")
        sys.stdout.flush()

    def px_line_graph(self, index, cluster_disc_resp):
        data = cluster_disc_resp.json()['mean']['metrics_summary']['metrics'][index]['data']
        df = pd.DataFrame.from_dict(data)
        df['date'] = pd.to_datetime(df['date'], origin='unix', unit='ms')
        if index == 0:
            fig = px.line(df, y=["capacity", "used"], x="date", height=400, title='CPU Very under-utilized and over-provisioned')
        else:
            fig = px.line(df, y=["capacity", "used"], x="date", height=400,
                          title='Memory Very under-utilized and over-provisioned')
        return fig

    def generate_cluster_info_values(self, cluster_disc_resp):
        cluster_info_values = [
            ['Cluster Name', 'Stack Type', 'Stack Version', 'Build Version', 'Kerberos', 'High Availability',
             'Services', 'Workflow Schedulers'],  # 1st col
            [cluster_disc_resp.json()['cluster_summary']['cluster_name'],
             cluster_disc_resp.json()['cluster_summary']['stack_type'],
             cluster_disc_resp.json()['cluster_summary']['stack_version'],
             cluster_disc_resp.json()['cluster_summary']['cluster_stack_build_version'],
             cluster_disc_resp.json()['cluster_summary']['is_kerberized'],
             cluster_disc_resp.json()['cluster_summary']['is_ha'],
             cluster_disc_resp.json()['cluster_summary']['services'],
             cluster_disc_resp.json()['cluster_summary']['workflow_schedulers']]]
        return cluster_info_values

    def figures_to_html(self, figs, filename="/opt/unravel/data/apps/unity-one/src/assets/reports/jobs/queue_analysis_test/20220808T063208/node_reduction.html"):
        with open(filename, 'w') as dashboard:
            dashboard.write("<html><head></head><body>" + "\n")
            for fig in figs:
                inner_html = fig.to_html(include_plotlyjs="cdn").split('<body>')[1].split('</body>')[0]
                dashboard.write(inner_html)
            dashboard.write("</body></html>" + "\n")

    def convert_size(self, size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def generate_table_fig(self, values, used_for):
        fig = go.Figure(data=[go.Table(
            columnorder=[1, 2],
            columnwidth=[80, 400],
            header=dict(
                values=[['<b>Environment</b><br>'],
                        ['<b>Details</b>']],
                align=['left', 'center'],
                font=dict(color='black', size=12),
                height=40
            ),
            cells=dict(
                values=values,
                align=['left', 'center'],
                font_size=12,
                height=30)
        )
        ])
        if used_for == 'CI':
            fig.update_layout(title_text='On Prem Cluster Identity'.format(self.percentile), title_x=0.5)
            fig.update_layout(height=500)
        else:
            fig.update_layout(title_text='Host Summary'.format(self.percentile), title_x=0.5)
            fig.update_layout(height=500)
        return fig

    def create_px_for_hosts(self, df):
        fig = px.bar(df, y=[ 'usage_cores', 'usage_Memory', 'usage_disk', 'spec_cores', 'spec_Memory', 'spec_disk'], x="Host", height=400,
                          title='Spec/Usage trend for selected hosts')
        return fig

    def create_host_fig(self, payload_unicode, key):
        payload = json.dumps(payload_unicode)
        main_df_list = []
        host_list = []
        host_roles_list = []
        usage_list = []
        actual_usage_list = []
        fig = None
        # payload = json.loads(payload)
        payload = eval(payload)
        payload = json.loads(payload)
        for hosts_data in payload[key]['hosts']:
            if self.filter_keys not in hosts_data['usage']['roles']:
                data_list = []
                host_list.append(hosts_data['id'])
                data_list.append(hosts_data['id'])
                host_roles_list.append(hosts_data['specs']['roles'])
                actual_usage_list.append(
                    'Cluster: {} <br>Cores: {} <br>Memory: {} <br>Disk: {}'.format(hosts_data['usage']['cluster_name'],
                                                                                   round(hosts_data['usage']['cores']),
                                                                                   self.convert_size(
                                                                                       hosts_data['usage']['memory_bytes']),
                                                                                   self.convert_size(
                                                                                       hosts_data['usage']['disk_bytes'])))
                data_list.append(round(hosts_data['usage']['cores']))
                data_list.append(hosts_data['usage']['memory_bytes'])
                data_list.append(hosts_data['usage']['disk_bytes'])





                usage_list.append(
                    'Cluster: {} <br>Cores: {} <br>Memory: {} <br>Disk: {}'.format(hosts_data['specs']['cluster_name'],
                                                                                   round(hosts_data['specs']['cores']),
                                                                                   self.convert_size(
                                                                                       hosts_data['specs']['memory_bytes']),
                                                                                   self.convert_size(
                                                                                       hosts_data['specs']['disk_bytes'])))
                data_list.append(round(hosts_data['specs']['cores']))
                data_list.append(hosts_data['specs']['memory_bytes'])
                data_list.append(hosts_data['specs']['disk_bytes'])
            main_df_list.append(data_list)
            fig = go.Figure(data=[go.Table(header=dict(values=['Host', 'Host Roles', 'Actual Usage', 'Capacity']),
                                           cells=dict(values=[host_list, host_roles_list,
                                                              actual_usage_list, usage_list]),),
                                  ])
            fig.update_layout(title_text='Host Details against {} usage percentile'.format(self.percentile), title_x=0.5)
            df = pd.DataFrame(main_df_list, columns=['Host', 'usage_cores', 'usage_Memory', 'usage_disk', 'spec_cores', 'spec_Memory', 'spec_disk'])
            fig2 = self.create_px_for_hosts(df)
        return fig2,fig

    def generate_avg_hosts_values(self, cluster_disc_resp):
        total_cores = 0
        total_disk = 0
        total_memory = 0
        count = 0
        avg_hosts_values = None
        for hosts_data in cluster_disc_resp.json()['hosts']:
            if self.filter_keys not in hosts_data['roles']:
                count = count + 1
                total_cores = total_cores + hosts_data['cores']
                total_disk = total_disk + hosts_data['disk_bytes']
                total_memory = total_memory + hosts_data['memory_bytes']
            avg_hosts_values = [['Hosts', 'Total Cores', 'Total Memory', 'Total Disk', 'Avg Cores/Host', 'Avg Memory/Host',
                              'Avg Disk/Host'],  # 1st col
                             [count, total_cores, self.convert_size(total_memory), self.convert_size(total_disk), total_cores / count,
                              self.convert_size(total_memory / count), self.convert_size(total_disk / count)]]
        return avg_hosts_values

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

    def generate(self):
        toolbar_width = 40
        sys.stdout.write("[%s]" % (" " * toolbar_width))
        sys.stdout.flush()
        sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
        key = ""
        if self.percentile == 100:
            key = 'p_100'
        elif self.percentile == 99:
            key = 'p_99'
        elif self.percentile == 95:
            key = 'p_95'
        elif self.percentile == 90:
            key = 'p_90'
        elif self.percentile == 85:
            key = 'p_85'
        elif self.percentile == 80:
            key = 'p_80'
        fig_list = []
        self.update_progress_bar()
        entity_id = self.generate_cluster_mapping_per_host_report()
        self.update_progress_bar()
        payload = self.get_report_payload(entity_id)
        self.update_progress_bar()
        cluster_disc_resp = requests.get(
            "https://gist.githubusercontent.com/Chandu-sanjith/eb453ee8ec2c810ba8fa87d2b8445399/raw/e1b768ef1daf4d38d0e2aa321f9136fb470e005a/Cluster%2520Disc")
        self.update_progress_bar()
        cluster_info_values = self.generate_cluster_info_values(cluster_disc_resp)
        self.update_progress_bar()
        fig_list.append(self.generate_table_fig(cluster_info_values, 'CI'))
        self.update_progress_bar()
        avg_hosts_values = self.generate_avg_hosts_values(cluster_disc_resp)
        self.update_progress_bar()
        fig_list.append(self.generate_table_fig(avg_hosts_values, 'CIIII'))
        self.update_progress_bar()
        fig_list.append(self.px_line_graph(0, cluster_disc_resp))
        self.update_progress_bar()
        fig_list.append(self.px_line_graph(1, cluster_disc_resp))
        self.update_progress_bar()
        # payload = requests.get(
        #     "https://gist.githubusercontent.com/Chandu-sanjith/77e067696662e73eba38cf15c7145fb3/raw/a98eae0f9ccfa7903d3e9ff8c0f8c6e17582bbbe/cloud%2520mapping%2520payload")
        fig1, fig2 = self.create_host_fig(payload, key)
        self.update_progress_bar()
        fig_list.append(fig1)
        self.update_progress_bar()
        fig_list.append(fig2)
        self.update_progress_bar()
        self.figures_to_html(fig_list)
        sys.stdout.write("]\n")
        print("Done and Dusted!!!!!")




def print_error_and_exit(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
    sys.exit(1)

if __name__ == "__main__":
    percentile_list = [100, 99, 95, 90, 85, 80]
    parser = argparse.ArgumentParser(description='Node reduction')
    parser.add_argument('--filter-keys', default=None, type=str,
                        help='Output file name to write data for interesting apps including scores (from the `score` '
                             'stage).')
    parser.add_argument('--percentile', default=None, type=int,
                        help='One of 100, 99, 95, 90, 85, 80')

    args = parser.parse_args()

    if args.percentile not in percentile_list:
        print_error_and_exit("percentile should be one of 100, 99, 95, 90, 85, 80 only....")

    NodeReduction(**vars(args)).generate()


