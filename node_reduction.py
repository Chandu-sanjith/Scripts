import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
import math
import argparse
import sys


class NodeReduction:
    def __init__(self, filter_keys, percentile):
        self.filter_keys = filter_keys
        self.percentile = percentile

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

    def figures_to_html(self, figs, filename="node_reduction.html"):
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

    def create_host_fig(self,payload, key):
        main_df_list = []
        host_list = []
        host_roles_list = []
        usage_list = []
        actual_usage_list = []
        fig = None
        for hosts_data in payload.json()[key]['hosts']:
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

    def generate(self):
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
        cluster_disc_resp = requests.get(
            "https://gist.githubusercontent.com/Chandu-sanjith/eb453ee8ec2c810ba8fa87d2b8445399/raw/e1b768ef1daf4d38d0e2aa321f9136fb470e005a/Cluster%2520Disc")
        cluster_info_values = self.generate_cluster_info_values(cluster_disc_resp)
        fig_list.append(self.generate_table_fig(cluster_info_values, 'CI'))
        avg_hosts_values = self.generate_avg_hosts_values(cluster_disc_resp)
        fig_list.append(self.generate_table_fig(avg_hosts_values, 'CIIII'))
        fig_list.append(self.px_line_graph(0, cluster_disc_resp))
        fig_list.append(self.px_line_graph(1, cluster_disc_resp))
        payload = requests.get(
            "https://gist.githubusercontent.com/Chandu-sanjith/77e067696662e73eba38cf15c7145fb3/raw/a98eae0f9ccfa7903d3e9ff8c0f8c6e17582bbbe/cloud%2520mapping%2520payload")
        fig1, fig2 = self.create_host_fig(payload, key)
        fig_list.append(fig1)
        fig_list.append(fig2)

        self.figures_to_html(fig_list)



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

