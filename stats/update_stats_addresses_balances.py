#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "zkbitcoin.com"
__copyright__ = "Copyright (C) 2021 zkbitcoin.com"
__license__ = "MIT License"
__version__ = "1.0"

import requests
import rocksdb
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import argparse
import json
import os


THREADS_COUNT = 20
skip_addresses = { 'STx39nArrm6fRBuo1QGm76Aax9YURGCiYi' : True, 'SMxvgbUZ1K5hEX1DVZq1KXpT7VArhY1iRZ': True }

parser = argparse.ArgumentParser()
parser.add_argument('-r', '--runfile-location-path',
                    required=False,
                    type=str,
                    default="../../../../run",
                    dest="rloc",
                    metavar="<runfile-location-path>",
                    help="runfile-location-path" )
parser.add_argument('-p', '--output-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-balances/json",
                    dest="p",
                    metavar="<output-location-path>",
                    help="output-location-pos-path")
parser.add_argument('-dr', '--db-rewards-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-rewards/db",
                    dest="dr",
                    metavar="--db-rewards-location-path>",
                    help="--db-rewards-location-path")
parser.add_argument('-db', '--db-balances-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-balances/db",
                    dest="db",
                    metavar="--db-balances-location-path>",
                    help="--db-balances-location-path")
parser.add_argument('-a', '--api-url',
                    required=False,
                    type=str,
                    default="http://127.0.0.1",
                    dest="apiurl",
                    metavar="<output-location-path>",
                    help="output-location-path")
parser.add_argument('-crt', '--crt',
                    required=False,
                    type=str,
                    default="../../../cert/CA/localhost/localhost.crt",
                    dest="crt",
                    metavar="<crt>",
                    help="crt")
parser.add_argument('-key', '--key',
                    required=False,
                    type=str,
                    default="../../../cert/CA/localhost/localhost.key",
                    dest="key",
                    metavar="<key>",
                    help="key")

args = parser.parse_args()

address_balances_run_file = os.path.join(args.rloc, "address_balances_run.json")
address_balances_file = os.path.join(args.p, "address_balances.json")

first_time = False

try:
    with open(address_balances_run_file, 'r') as f:
        address_balances_run = json.load(f)
except FileNotFoundError:
    options = rocksdb.Options()
    options.create_if_missing = True
    db = rocksdb.DB(args.db, options, column_families={}, read_only=False)
    db.create_column_family(b'address_balances_cf', rocksdb.ColumnFamilyOptions())
    del db
    address_rewards_run = {"run":True}
    first_time = True

db_rewards_column_families = {
    b'default': rocksdb.ColumnFamilyOptions(),
    b'address_rewards_pos_cf': rocksdb.ColumnFamilyOptions(),
    b'address_rewards_mn_cf': rocksdb.ColumnFamilyOptions(),
}

db_balances_column_families = {
    b'default': rocksdb.ColumnFamilyOptions(),
    b'address_balances_cf': rocksdb.ColumnFamilyOptions(),
}

options = rocksdb.Options()
options.create_if_missing = False
dbr = rocksdb.DB(args.dr, options, column_families=db_rewards_column_families, read_only=True)
options = rocksdb.Options()
options.create_if_missing = True
dbb = rocksdb.DB(args.db, options, column_families=db_balances_column_families, read_only=False)

address_rewards_pos_cf = dbr.get_column_family(b'address_rewards_pos_cf')
address_rewards_mn_cf = dbr.get_column_family(b'address_rewards_mn_cf')

address_balances_cf = dbr.get_column_family(b'address_balances_cf')

address_balances = {}

class BlockbookClient:
    def __init__(self):
        self.url = args.apiurl
        self.url = self.url + "/api/v2/balancehistory"
        self.s = requests.Session()
        self.retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
        self.s.mount('http://', HTTPAdapter(max_retries=self.retries))

    def check_response(self, address, param=""):
        url = self.url + "/%s" % (address)
        if param != "":
            url += "?" % param
        headers = {
            'Accept': "application/json",
        }
        if args.apiurl.startswith('https'):
            resp = self.s.get(url, data={}, verify=False, cert=(args.crt, args.key))
        else:
            resp = self.s.get(url, data={}, verify=True)
        if resp.status_code == 200:
            data = resp.json()
            return data
        if resp.status_code == 400: # has not been indexed as yet
            return None
        raise Exception("Invalid response: %s  url: %s" % (str(resp), url))

    def get_balance_history(self, address, param=""):
        return self.check_response(address)


def db_to_array():
    it = dbb.iteritems(address_balances_cf)
    it.seek_to_first()
    for i in it:
        k = str(i[0], "utf-8")
        v = i[1]
        address_balances[k] = {}
        address_balances[k] = eval(v.decode("utf-8"))


def update(i, d):
    address_balances_array = []
    b = json.loads(json.dumps(d))
    for e in b:
        address_balances_array.append({'t': int(e["time"]), "r" : int(e["received"]), "s" : int(e["sent"]), "ss" : int(e["sentToSelf"])})
    address_balances[i] = address_balances_array
    dbb.put((address_balances_cf, bytes(i, 'utf-8')), bytes(str(address_balances_array), 'utf-8'))


def process(address_array):
    bb_conn = BlockbookClient()
    for i in address_array:
        if i in skip_addresses:
            continue
        if i in address_balances:
            if len(address_balances[i]) > 0:
                d = bb_conn.get_balance_history(i, param="from=" + str(address_balances[i][-1]['t'] + 86400) + "&groupBy=86400")
                if d is None:
                    continue
                update(i, d)
                continue
        address_balances[i] = {}
        d = bb_conn.get_balance_history(i, "&groupBy=86400")
        if d is None:
            continue
        update(i, d)


class ProcessThread(threading.Thread):
    def run(self):
        address_array = self._args["address_array"]
        print("{} started, processing {} addresses".format(self.getName(), len(address_array)))
        process(address_array)


def main():

    thread_array = []
    address_array = []
    address_added = {}

    if not first_time:
        db_to_array()

    it = dbr.iterkeys(address_rewards_pos_cf)
    it.seek_to_first()
    for i in it:
        key = i[1].decode("utf-8")
        address_array.append(key)
        address_added[key] = True
    it = dbr.iterkeys(address_rewards_mn_cf)
    it.seek_to_first()
    for i in it:
        key = i[1].decode("utf-8")
        if key not in address_added:
            address_array.append(key)

    l = len(address_array)
    e = int(l / THREADS_COUNT)

    beg = 0
    for i in range(THREADS_COUNT):
        if i < THREADS_COUNT - 1:
            end = beg + e
        else:
            end = l + 1
        thread = ProcessThread(name = "Thread-{}".format(i), args={'address_array' : address_array[beg:end]})
        thread_array.append(thread)
        beg = e + beg

    for i in range(len(thread_array)):
        thread_array[i].start()

    for i in range(len(thread_array)):
        thread_array[i].join()

    address_balances_array  = []
    for k, v in address_balances.items():
        address_balances_array.append({"a": k, "dc": v})

    with open(address_balances_file, 'w+') as f:
        json.dump(address_balances_array , f)

    with open(address_balances_run_file, 'w+') as f:
        address_balances_run = {}
        address_balances_run["run"] = True
        json.dump(address_balances_run, f)


if __name__ == "__main__":
    main()