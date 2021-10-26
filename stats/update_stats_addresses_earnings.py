#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://github-wiki-see.page/m/facebook/rocksdb/wiki/Column-Families

from bitcoinrpc.authproxy import AuthServiceProxy
import rocksdb
import ast

try:
    import http.client as httplib
except ImportError:
    import httplib
import json
import os
import urllib3
import argparse
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime
from collections import OrderedDict

isTestnet = False

rpc_user = "rpc"
rpc_pass = "pivxrpc"
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
                    default="../../../../stats_data/address-stake/json",
                    dest="oloc",
                    metavar="<output-location-path>",
                    help="output-location-path")
parser.add_argument('-d', '--db-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-stake/db",
                    dest="dbloc",
                    metavar="<output-location-path>",
                    help="output-location-path")

PROCESS_BLOCK_COUNTERS = 0
PROCESS_DATE_COUNTERS = 1

args = parser.parse_args()

address_stake_run_file = os.path.join(args.rloc, "address_stake_run.json")
address_stake_file = os.path.join(args.oloc, "address_stake.json")

options = rocksdb.Options()
options.create_if_missing = True

db = rocksdb.DB(args.dbloc, options, read_only=False)

first_time = False

try:
    with open(address_stake_run_file, 'r') as f:
        address_stake_run = json.load(f)
except FileNotFoundError:
    address_stake_run = {"block":1}
    first_time = True

# Initialize RPC connection and API client
httpConnection = httplib.HTTPConnection(rpc_host, rpc_port, timeout=20)
conn = AuthServiceProxy(rpc_url, timeout=1000, connection=httpConnection)
blockCount = conn.getblockcount()
addresses = {}

def db_to_date_counters():
    it = db.iteritems()
    it.seek_to_first()
    for i in it:
        k = str(i[0], "utf-8")
        v = i[1]
        addresses[k] = {}
        addresses[k].update(ast.literal_eval(v.decode("utf-8")))
        #print("Address %s date_counters %s " % (k, v))
    #print(addresses)


def db_prune():
    it = db.iteritems()
    it.seek_to_first()
    for i in it:
        k = str(i[0], "utf-8")
        if k is not addresses:
            #print("Delete %s address from db " % (k))
            db.delete(i[0])

# first block of 2021 2660000

def update_counters(address, pattern, date):
    found = False
    for e in addresses[address]['date_counters'][pattern]:
        if e['date'] == date:
            found = True
            e['count'] = e['count'] + 2
    if not found:
        addresses[address]['date_counters']['ymdH'].append({'date': date, 'count': 2})

def get_counters(process_type, block_start, block_stop, order):
    j = 0
    for i in range(block_start, block_stop, order):
        j += 1
        print("Getting block %d..." % i)
        block_hash = conn.getblockhash(i)
        block = conn.getblock(block_hash, True)
        for tx_hash in block['tx']:
            tx_raw = conn.getrawtransaction(tx_hash, True)
            stake = False
            for vout in tx_raw['vout']:
                if "type" in vout["scriptPubKey"]:
                    type = vout["scriptPubKey"]["type"]
                    if type == "nonstandard":
                        stake = True
                    else:
                        if "addresses" in vout["scriptPubKey"]:
                            if stake:
                                tx_addresses = vout["scriptPubKey"]["addresses"]
                                address = tx_addresses[0]
                                if process_type == PROCESS_BLOCK_COUNTERS:
                                    if address not in addresses:
                                        addresses[address] = {}
                                    if 'block_counters' not in  addresses[address]:
                                        addresses[address]['block_counters'] = [0,0,0,0,0]
                                    if j <= 50:
                                        k = 0
                                    elif j <= 250:
                                        k = 1
                                    elif j <= 500:
                                        k = 2
                                    elif j <= 750:
                                        k = 3
                                    else:
                                        k = 4
                                    for l in range(k, 5):
                                        addresses[address]['block_counters'][l] = addresses[address]['block_counters'][l] + 2
                                    #print("Updating Address %s block counters %s " % (address, block_counters[address]))
                                else: # PROCESS_DATE_COUNTERS
                                    if address not in addresses:
                                        continue
                                    date = datetime.fromtimestamp(int(tx_raw["time"]))
                                    ymdH = date.strftime("%Y-%m-%d %H")
                                    ymd = date.strftime("%Y-%m-%d")
                                    ym = date.strftime("%Y-%m")
                                    y = date.strftime("%Y")
                                    if 'date_counters' not in  addresses[address]:
                                        addresses[address]['date_counters'] = {}
                                        addresses[address]['date_counters']['ymdH'] = []
                                        addresses[address]['date_counters']['ymd'] = []
                                        addresses[address]['date_counters']['ym'] = []
                                        addresses[address]['date_counters']['y'] = []
                                    update_counters(address, 'ymdH', ymdH)
                                    update_counters(address, 'ymd', ymd)
                                    update_counters(address, 'ym', ym)
                                    update_counters(address, 'y', y)
                                    #print("Updating Address %s date_counters %s " % (address, addresses[address]))
                                    db.put(bytes(address, 'utf-8'), bytes(str(addresses[address]), 'utf-8'))
                            break

get_counters(PROCESS_BLOCK_COUNTERS, blockCount, blockCount-1000, -1)

if first_time:
    get_counters(PROCESS_DATE_COUNTERS, 2660000, blockCount, +1)
else:
    with open(address_stake_run_file, 'r') as json_data:
        d = json.load(json_data)
        json_data.close()
        db_to_date_counters()
        get_counters(PROCESS_DATE_COUNTERS, d["block"]+1, blockCount, +1)

try:
    address_stake_array = []

    for k, v in addresses.items():
        address_stake = {"address": k, "counters": v}
        address_stake_array.append(address_stake)

    address_stake_array = sorted(address_stake_array, key = lambda i: i["counters"]["block_counters"][4], reverse=True)

    with open(address_stake_file, 'w+') as f:
        json.dump(address_stake_array, f)

    #print(json.dumps(address_stake_array))

    address_stake_run["block"] = blockCount
except:
    raise
finally:
    with open(address_stake_run_file, 'w+') as f:
        address_stake_run["block"]
        json.dump(address_stake_run, f)

httpConnection.close()
