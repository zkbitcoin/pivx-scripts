#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://github-wiki-see.page/m/facebook/rocksdb/wiki/Column-Families

from bitcoinrpc.authproxy import AuthServiceProxy
import rocksdb
import argparse

try:
    import http.client as httplib
except ImportError:
    import httplib
import json
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import threading
import time

isTestnet = False

rpc_user = "<user>"
rpc_pass = "<password>"
rpc_host = "127.0.0.1"
rpc_port = "18049" if isTestnet else "8049"
rpc_url = "http://%s:%s@%s" % (rpc_user, rpc_pass, rpc_host)

parser = argparse.ArgumentParser()
parser.add_argument('-r', '--runfile-location-path',
                    required=False,
                    type=str,
                    default="../../../../run",
                    dest="rloc",
                    metavar="<runfile-location-path>",
                    help="runfile-location-path" )
parser.add_argument('-o', '--output-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-active/json",
                    dest="oloc",
                    metavar="<output-location-path>",
                    help="output-location-path")
parser.add_argument('-d', '--db-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-active/db",
                    dest="dbloc",
                    metavar="<output-location-path>",
                    help="output-location-path")
parser.add_argument('-a', '--api-url',
                    required=False,
                    type=str,
                    default="http://127.0.0.1",
                    dest="apiurl",
                    metavar="<output-location-path>",
                    help="output-location-path")


args = parser.parse_args()

address_balances_active_run_file = os.path.join(args.rloc, "address_active_run.json")
address_balances_active_file = os.path.join(args.oloc, "address_active.json")

BLOCK_COUNT = 7 * 1440
THREADS_COUNT = 10

first_time = False

try:
    with open(address_balances_active_run_file, 'r') as f:
        address_balance_run = json.load(f)
except FileNotFoundError:
    # first run (hardcoded)
    print("First run...")
    first_time = True
    address_balance_run = {}
    address_balance_run["block"] = 0

processed_total = 0

# Initialize RPC connection and API client
httpConnection = httplib.HTTPConnection(rpc_host, rpc_port, timeout=5)
conn = AuthServiceProxy(rpc_url, timeout=30, connection=httpConnection)

blockCount = conn.getblockcount()

address_counters = {}

def process(thread_name, start_block, end_block):
    httpConnection_t = httplib.HTTPConnection(rpc_host, rpc_port, timeout=5)
    conn_t = AuthServiceProxy(rpc_url, timeout=30, connection=httpConnection_t)
    for i in range (start_block, end_block):
        global processed_total
        processed_total = processed_total + 1
        #if processed_total % 1000 == 0:
        #print("total processed %i..." % (processed_total))
        #print("Thread %s Getting block %d..." % (thread_name, i))
        block = {}
        for j in range(0,30):
            try:
                block_hash = conn_t.getblockhash(i)
                block = conn_t.getblock(block_hash, True)
                break
            except Exception as e:
                #print(e)
                time.sleep(5)
        block = conn_t.getblock(block_hash, True)
        for tx_hash in block['tx']:
            for j in range(0,30):
                try:
                    tx_raw = conn_t.getrawtransaction(tx_hash, True)
                    break
                except Exception as e:
                    #print(e)
                    time.sleep(5)
            for vout in tx_raw['vout']:
                if "addresses" in vout["scriptPubKey"]:
                    tx_addresses = vout["scriptPubKey"]["addresses"]
                    for address in tx_addresses:
                        if address not in address_counters:
                            address_counters[address] = {}
                            address_counters[address]["count"] = 1
                        else:
                            address_counters[address]["count"] =  address_counters[address]["count"] + 1
    httpConnection_t.close()

class ProcessThread(threading.Thread):
    def run(self):
        start_block = self._args["start_block"]
        end_block = self._args["end_block"]
        #print("{} processing from block: {} to block: {}".format(self.getName(), start_block, end_block))
        process(self.getName(), start_block, end_block)


def main():

    thread_array = []

    l = (blockCount - (BLOCK_COUNT))
    e = int((BLOCK_COUNT) / THREADS_COUNT)

    beg = l

    for i in range(THREADS_COUNT):
        if i < THREADS_COUNT - 1:
            end = beg + e
        else:
            end = blockCount + 1
        thread = ProcessThread(name = "Thread-{}".format(i), args={'start_block' : beg, 'end_block' : end})
        thread_array.append(thread)
        beg = e + beg

    address_balance_run["block"] = blockCount

    for i in range(len(thread_array)):
        thread_array[i].start()

    for i in range(len(thread_array)):
        thread_array[i].join()

    address_counters_array  = []
    for k, v in address_counters.items():
        address_counters_array.append({"address": k, "count": v['count']})

    address_counters_array = sorted(address_counters_array, key = lambda i: i["count"], reverse=True)

    with open(address_balances_active_file, 'w+') as f:
        json.dump(address_counters_array , f)

    with open(address_balances_active_run_file, 'w+') as f:
        address_balance_run["block"]
        json.dump(address_balance_run, f)

    httpConnection.close()

if __name__ == "__main__":
    main()
