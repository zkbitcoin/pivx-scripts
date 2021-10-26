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
parser.add_argument('-posp', '--output-location-pos-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-earnings/pos/json",
                    dest="posloc",
                    metavar="<output-location-pos-path>",
                    help="output-location-pos-path")
parser.add_argument('-mnp', '--output-location-mn-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-earnings/mn/json",
                    dest="mnloc",
                    metavar="<output-location-mn-path>",
                    help="output-location-mn-path")
parser.add_argument('-d', '--db-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-earnings/db",
                    dest="dbloc",
                    metavar="<output-location-path>",
                    help="output-location-path")

ADDRESSES_POS = 0
ADDRESSES_MN = 1
PROCESS_BLOCK_COUNTERS = 0
PROCESS_DATE_COUNTERS = 1


args = parser.parse_args()

address_earnings_run_file = os.path.join(args.rloc, "address_earnings_run.json")
address_earnings_file = [os.path.join(args.posloc, "address_earnings_pos.json"), os.path.join(args.mnloc, "address_earnings_mn.json")]

options = rocksdb.Options()
options.create_if_missing = True

first_time = False

try:
    with open(address_earnings_run_file, 'r') as f:
        address_earnings_run = json.load(f)
except FileNotFoundError:
    db = rocksdb.DB(args.dbloc, options, column_families={}, read_only=False)
    db.create_column_family(b'address_earnings_pos_cf', rocksdb.ColumnFamilyOptions())
    db.create_column_family(b'address_earnings_mn_cf', rocksdb.ColumnFamilyOptions())
    del db
    address_earnings_run = {"block":1}
    first_time = True

column_families = {
    b'default': rocksdb.ColumnFamilyOptions(),
    b'address_earnings_pos_cf': rocksdb.ColumnFamilyOptions(),
    b'address_earnings_mn_cf': rocksdb.ColumnFamilyOptions(),
}

db = rocksdb.DB(args.dbloc, options, column_families=column_families, read_only=False)
address_earnings_pos_cf = db.get_column_family(b'address_earnings_pos_cf')
address_earnings_mn_cf = db.get_column_family(b'address_earnings_mn_cf')

# Initialize RPC connection and API client
httpConnection = httplib.HTTPConnection(rpc_host, rpc_port, timeout=20)
conn = AuthServiceProxy(rpc_url, timeout=1000, connection=httpConnection)
blockCount = conn.getblockcount()

address_array = [{}, {}]

def db_to_date_counters(addresses, columnFamily):
    it = db.iteritems(columnFamily)
    it.seek_to_first()
    for i in it:
        k = str(i[0], "utf-8")
        v = i[1]
        addresses[k] = {}
        addresses[k].update(ast.literal_eval(v.decode("utf-8")))
        #print("Address %s date_counters %s " % (k, v))
    #print(addresses)


def db_prune(addresses):
    it = db.iteritems()
    it.seek_to_first()
    for i in it:
        k = str(i[0], "utf-8")
        if k is not addresses:
            #print("Delete %s address from db " % (k))
            db.delete(i[0])

# first block of 2021 2660000


def update_counters(addresses, address, pattern, date, reward):
    found = False
    for e in addresses[address]['date_counters'][pattern]:
        if e['date'] == date:
            found = True
            e['count'] = e['count'] + reward
    if not found:
        addresses[address]['date_counters'][pattern].append({'date': date, 'count': reward})

def get_counters(process_type, block_start, block_stop, order):
    j = 0
    for i in range(block_start, block_stop, order):
        j += 1
        #print("Getting block %d..." % i)
        block_hash = conn.getblockhash(i)
        block = conn.getblock(block_hash, True)
        for tx_hash in block['tx']:
            tx_raw = conn.getrawtransaction(tx_hash, True)
            i = ADDRESSES_POS
            stake = False
            for vout in tx_raw['vout']:
                if "type" in vout["scriptPubKey"]:
                    type = vout["scriptPubKey"]["type"]
                    if type == "nonstandard":
                        stake = True
                        continue
                    if stake:
                        if "addresses" in vout["scriptPubKey"]:
                            reward = 0
                            if i == ADDRESSES_POS:
                                reward = 2
                            else:
                                reward = 3
                            tx_addresses = vout["scriptPubKey"]["addresses"]
                            address = tx_addresses[0]
                            if process_type == PROCESS_BLOCK_COUNTERS:
                                if address not in address_array[i]:
                                    address_array[i][address] = {}
                                if 'block_counters' not in  address_array[i][address]:
                                    address_array[i][address]['block_counters'] = [0,0,0,0,0]
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
                                    address_array[i][address]['block_counters'][l] = address_array[i][address]['block_counters'][l] + reward
                                #if i == ADDRESSES_POS:
                                #    print("Updating POS Address %s block counters %s " % (address, address_array[i][address]['block_counters']))
                                #else:
                                #    print("Updating Masternode Address %s block counters %s " % (address, address_array[i][address]['block_counters']))
                            else: # PROCESS_DATE_COUNTERS
                                if address not in address_array[i]:
                                    continue
                                date = datetime.fromtimestamp(int(tx_raw["time"]))
                                ymdH = date.strftime("%Y-%m-%d %H")
                                ymd = date.strftime("%Y-%m-%d")
                                ym = date.strftime("%Y-%m")
                                y = date.strftime("%Y")
                                if 'date_counters' not in  address_array[i][address]:
                                    address_array[i][address]['date_counters'] = {}
                                    address_array[i][address]['date_counters']['ymdH'] = []
                                    address_array[i][address]['date_counters']['ymd'] = []
                                    address_array[i][address]['date_counters']['ym'] = []
                                    address_array[i][address]['date_counters']['y'] = []
                                update_counters(address_array[i], address, 'ymdH', ymdH, reward)
                                update_counters(address_array[i], address, 'ymd', ymd, reward)
                                update_counters(address_array[i], address, 'ym', ym, reward)
                                update_counters(address_array[i], address, 'y', y, reward)
                                if i == ADDRESSES_POS:
                                    db.put((address_earnings_pos_cf, bytes(address, 'utf-8')), bytes(str(address_array[i][address]), 'utf-8'))
                                    #print("Updating POS Address %s date_counters %s " % (address, address_array[i][address]))
                                else:
                                    db.put((address_earnings_mn_cf, bytes(address, 'utf-8')), bytes(str(address_array[i][address]), 'utf-8'))
                                    #print("Updating Masternode Address %s date_counters %s " % (address, address_array[i][address]))
                            if (i := (i + 1)) > 1:
                                break

get_counters(PROCESS_BLOCK_COUNTERS, blockCount, blockCount-1000, -1)

if first_time:
    get_counters(PROCESS_DATE_COUNTERS, 2660000, blockCount, +1)
else:
    with open(address_earnings_run_file, 'r') as json_data:
        d = json.load(json_data)
        json_data.close()
        db_to_date_counters(address_array[ADDRESSES_POS], address_earnings_pos_cf)
        db_to_date_counters(address_array[ADDRESSES_MN], address_earnings_mn_cf)
        get_counters(PROCESS_DATE_COUNTERS, d["block"]+1, blockCount, +1)

try:
    for i in range (ADDRESSES_POS, ADDRESSES_MN+1):
        address_earnings_array  = []

        for k, v in address_array[i].items():
            address_earnings_array.append({"address": k, "counters": v})

        address_earnings_array = sorted(address_earnings_array, key = lambda i: i["counters"]["block_counters"][4], reverse=True)

        with open(address_earnings_file[i], 'w+') as f:
            json.dump(address_earnings_array , f)

except:
    raise
finally:
    with open(address_earnings_run_file, 'w+') as f:
        address_earnings_run["block"] = blockCount
        json.dump(address_earnings_run, f)

httpConnection.close()
