import csv
import json
import time
import pandas as pd
import feather
import numpy as np
import argparse
import random
import glob
import os

from datetime import datetime, timezone, timedelta
from pathlib import Path

from multiprocessing import Pool
from pandarallel import pandarallel


class FsimageProcessor:

    def __init__(self, args):
        self.fsimage_path = args.fsimage_path
        self.chunk_size = args.chunks
        self.featherbolt_dir_path = args.featherbolt_dir_path
        self.retention = args.retention

    def check_if_parent(self, path):
        path_obj = Path(path)
        if len(path_obj.parents) == 2:
            return True
        else:
            return False

    def check_path(self, df):
        df['is_parent'] = df['Path'].apply(self.check_if_parent)
        return df

    def parallelize_dataframe(self, df, func, n_cores=20):
        df_split = np.array_split(df, n_cores)
        pool = Pool(n_cores)
        df = pd.concat(pool.map(func, df_split))
        pool.close()
        pool.join()
        return df

    def process_chunk(self, df, timestamp, chunk_count):
        df = df[pd.to_numeric(df['FileSize'], errors='coerce').notnull()]
        df = df.reset_index(drop=True)
        df = self.parallelize_dataframe(df, self.check_path)
        parent_tenant_df = pd.DataFrame(df.Path.str.split('/', 3).tolist(),
                                        columns=['dummy1', 'project', 'tenant', 'dummy2'])
        parent_tenant_df.drop('dummy1', axis=1, inplace=True)
        parent_tenant_df.drop('dummy2', axis=1, inplace=True)
        df['project'] = parent_tenant_df['project']
        df['tenant'] = parent_tenant_df['tenant']
        df['date'] = timestamp
        df['date'] = df['date'].astype("int")
        df['Path'] = df["Path"].astype("string")
        df['FileSize'] = df["FileSize"].astype("int")
        df['NSQUOTA'] = df["NSQUOTA"].astype("int")
        df['DSQUOTA'] = df["DSQUOTA"].astype("int")
        df['is_parent'] = df["is_parent"].astype("string")
        df['project'] = df["project"].astype("string")
        df['tenant'] = df["tenant"].astype("string")
        print("time taken to process fs image {}".format(time.time() - timestamp))
        out_file = "{}/{}.featherbolt_{}".format(self.featherbolt_dir_path, timestamp, chunk_count)
        df.to_feather(out_file)
        print("time taken write {}".format(time.time() - timestamp))

    def clear_retention_breached_files(self):
        list_of_files = []
        flag = 0
        end_ts = datetime.now(tz=timezone.utc)
        start_ts = end_ts - timedelta(days=self.retention)
        start_unix_ts = int(start_ts.strftime("%s"))
        for file in glob.glob('{}/*'.format(self.featherbolt_dir_path)):
            flag = 1
            file_date = int(file.split("/")[-1].split(".")[0])
            if file_date < start_unix_ts:
                list_of_files.append(file)
        if not list_of_files and flag == 1:
            print("no data for the given days")
        else:
            for file_to_be_removed in list_of_files:
                os.remove(file_to_be_removed)
            print("removed files {}".format(list_of_files))

    def generate(self):
        chunk_count = 0
        timestamp = int(time.time())
        chunksize = int(self.chunk_size)
        print("starting now {}".format(time.time() - timestamp))
        for chunk in pd.read_csv(self.fsimage_path, sep='\t', usecols=['Path', 'FileSize', 'NSQUOTA', 'DSQUOTA'],
                                 chunksize=chunksize, iterator=True, low_memory=False):
            self.process_chunk(chunk, timestamp, chunk_count)
            chunk_count = chunk_count + 1
        print("TOTAL TIME taken write {}".format(time.time() - timestamp))
        self.clear_retention_breached_files()


if __name__ == '__main__':
    pandarallel.initialize()
    parser = argparse.ArgumentParser(description='Process fsimage and compress it')
    parser.add_argument('--chunks', type=str, default=None,
                        help='chunk size to partion the fsimage.txt to process it.')
    parser.add_argument('--fsimage-path', type=str, default=None,
                        help='Path where fsimage.txt is present')
    parser.add_argument('--featherbolt-dir-path', type=str, default=None,
                        help='Feather Bolt directory path where processed fsimage should be stored')
    parser.add_argument('--retention', type=int, default=8,
                        help='Feather Bolt files will be deleted once they breach retention period')
    args = parser.parse_args()
    FsimageProcessor(args).generate()