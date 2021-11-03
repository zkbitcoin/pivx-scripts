#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "zkbitcoin.com"
__copyright__ = "Copyright (C) 2021 zkbitcoin.com"
__license__ = "MIT License"
__version__ = "1.0"

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
                    default="../../../../stats_data/address-rewards/pos/json",
                    dest="posloc",
                    metavar="<output-location-pos-path>",
                    help="output-location-pos-path")
parser.add_argument('-mnp', '--output-location-mn-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-rewards/mn/json",
                    dest="mnloc",
                    metavar="<output-location-mn-path>",
                    help="output-location-mn-path")
parser.add_argument('-d', '--db-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-rewards/db",
                    dest="dbloc",
                    metavar="<output-location-path>",
                    help="output-location-path")
MAX_BLOCK_DEPTH = 1000
ADDRESSES_POS = 0
ADDRESSES_MN = 1
PROCESS_BLOCK_COUNTERS = 0
PROCESS_DATE_COUNTERS = 1


args = parser.parse_args()

address_rewards_run_file = os.path.join(args.rloc, "address_rewards_run.json")
address_rewards_block_counters_file = [os.path.join(args.posloc, "address_rewards_block_counters_pos.json"), os.path.join(args.mnloc, "address_rewards_block_counters_mn.json")]
address_rewards_date_counters_file = [os.path.join(args.posloc, "address_rewards_date_counters_pos.json"), os.path.join(args.mnloc, "address_rewards_date_counters_mn.json")]


options = rocksdb.Options()
options.create_if_missing = True

first_time = False

try:
    with open(address_rewards_run_file, 'r') as f:
        address_rewards_run = json.load(f)
except FileNotFoundError:
    db = rocksdb.DB(args.dbloc, options, column_families={}, read_only=False)
    db.create_column_family(b'address_rewards_pos_cf', rocksdb.ColumnFamilyOptions())
    db.create_column_family(b'address_rewards_mn_cf', rocksdb.ColumnFamilyOptions())
    del db
    address_rewards_run = {"block":1}
    first_time = True

column_families = {
    b'default': rocksdb.ColumnFamilyOptions(),
    b'address_rewards_pos_cf': rocksdb.ColumnFamilyOptions(),
    b'address_rewards_mn_cf': rocksdb.ColumnFamilyOptions(),
}

db = rocksdb.DB(args.dbloc, options, column_families=column_families, read_only=False)
address_rewards_pos_cf = db.get_column_family(b'address_rewards_pos_cf')
address_rewards_mn_cf = db.get_column_family(b'address_rewards_mn_cf')

# Initialize RPC connection and API client
httpConnection = httplib.HTTPConnection(rpc_host, rpc_port, timeout=20)
conn = AuthServiceProxy(rpc_url, timeout=1000, connection=httpConnection)
blockCount = conn.getblockcount()

address_array = [{}, {}]
block_counters = []
block_counters = [{} for i in range(PROCESS_DATE_COUNTERS+1)]
date_counters = []
date_counters = [{} for i in range(PROCESS_DATE_COUNTERS+1)]


def db_to_date_counters(counters, columnFamily):
    it = db.iteritems(columnFamily)
    it.seek_to_first()
    for i in it:
        k = str(i[0][1], "utf-8")
        v = i[1]
        address_array[counters][k] = {}
        address_array[counters][k].update(ast.literal_eval(v.decode("utf-8")))
        date_counters[counters][k] = {}
        date_counters[counters][k]['ymdH'] = OrderedDict()
        date_counters[counters][k]['ymd'] = OrderedDict()
        date_counters[counters][k]['ym'] = OrderedDict()
        date_counters[counters][k]['y'] = OrderedDict()

        for elem in address_array[counters][k]['dc']['ymdH']:
            date_counters[counters][k]['ymdH'].update({elem['d']: elem['c']})
        for elem in address_array[counters][k]['dc']['ymd']:
            date_counters[counters][k]['ymd'].update({elem['d']: elem['c']})
        for elem in address_array[counters][k]['dc']['ym']:
            date_counters[counters][k]['ym'].update({elem['d']: elem['c']})
        for elem in address_array[counters][k]['dc']['y']:
            date_counters[counters][k]['y'].update({elem['d']: elem['c']})

        #print("Address %s date_counters %s " % (k, v))
    #print(addresses)


def db_prune(addresses):
    it = db.iteritems()
    it.seek_to_first()
    for i in it:
        k = str(i[0][1], "utf-8")
        if k is not addresses:
            #print("Delete %s address from db " % (k))
            db.delete(i[0])

# first block of 2021 2660000


def update_counters(addresses, address, pattern, date, reward):
    found = False
    for e in addresses[address]['dc'][pattern]:
        if e['d'] == date:
            found = True
            e['c'] = e['c'] + reward
    if not found:
        addresses[address]['dc'][pattern].append({'d': date, 'c': reward})

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
                            if i == ADDRESSES_POS:
                                reward = 2
                            else:
                                reward = 3
                            tx_addresses = vout["scriptPubKey"]["addresses"]
                            address = tx_addresses[0]
                            if process_type == PROCESS_BLOCK_COUNTERS:
                                if address not in block_counters[i]:
                                    address_array[i][address] = {}
                                    block_counters[i][address] = [0,0,0,0,0]
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
                                    block_counters[i][address][l] = block_counters[i][address][l] + reward
                                address_array[i][address]['bc'] = block_counters[i][address]
                                #if i == ADDRESSES_POS:
                                #    print("Updating POS Address %s block counters %s " % (address, address_array[i][address]['bc']))
                                #else:
                                #    print("Updating Masternode Address %s block counters %s " % (address, address_array[i][address]['bc']))
                            else: # PROCESS_DATE_COUNTERS
                                if address not in block_counters[i]:
                                    continue
                                date = datetime.fromtimestamp(int(tx_raw["time"]))
                                ymdH = date.strftime("%Y-%m-%d %H")
                                ymd = date.strftime("%Y-%m-%d")
                                ym = date.strftime("%Y-%m")
                                y = date.strftime("%Y")
                                if address not in date_counters[i]:
                                    address_array[i][address]["dc"] = {}
                                    date_counters[i][address] = {}
                                if 'ymdH' not in date_counters[i][address]:
                                    date_counters[i][address]['ymdH'] = OrderedDict()
                                if 'ymd' not in date_counters[i][address]:
                                    date_counters[i][address]['ymd'] = OrderedDict()
                                if 'ym' not in date_counters[i][address]:
                                    date_counters[i][address]['ym'] = OrderedDict()
                                if 'y' not in date_counters[i][address]:
                                    date_counters[i][address]['y'] = OrderedDict()
                                if ymdH not in date_counters[i][address]['ymdH']:
                                    date_counters[i][address]['ymdH'][ymdH] = 0
                                if ymd not in date_counters[i][address]['ymd']:
                                    date_counters[i][address]['ymd'][ymd] = 0
                                if ym not in date_counters[i][address]['ym']:
                                    date_counters[i][address]['ym'][ym] = 0
                                if y not in date_counters[i][address]['y']:
                                    date_counters[i][address]['y'][y] = 0

                                date_counters[i][address]['ymdH'].update({ymdH: date_counters[i][address]['ymdH'][ymdH] + reward})
                                date_counters[i][address]['ymd'].update({ymd: date_counters[i][address]['ymd'][ymd] + reward})
                                date_counters[i][address]['ym'].update({ym: date_counters[i][address]['ym'][ym] + reward})
                                date_counters[i][address]['y'].update({y: date_counters[i][address]['y'][y] + reward})

                                address_array[i][address]["dc"]['ymdH'] = [{'d': key, 'c': value} for key, value in date_counters[i][address]['ymdH'].items()]
                                address_array[i][address]["dc"]['ymd'] = [{'d': key, 'c': value} for key, value in date_counters[i][address]['ymd'].items()]
                                address_array[i][address]["dc"]['ym'] = [{'d': key, 'c': value} for key, value in date_counters[i][address]['ym'].items()]
                                address_array[i][address]["dc"]['y'] = [{'d': key, 'c': value} for key, value in date_counters[i][address]['y'].items()]

                                if i == ADDRESSES_POS:
                                    db.put((address_rewards_pos_cf, bytes(address, 'utf-8')), bytes(str(address_array[i][address]), 'utf-8'))
                                    #print("Updating POS Address %s date_counters %s " % (address, address_array[i][address]))
                                else:
                                    db.put((address_rewards_mn_cf, bytes(address, 'utf-8')), bytes(str(address_array[i][address]), 'utf-8'))
                                    #print("Updating Masternode Address %s date_counters %s " % (address, address_array[i][address]))
                            if (i := (i + 1)) > 1:
                                break

get_counters(PROCESS_BLOCK_COUNTERS, blockCount, blockCount-MAX_BLOCK_DEPTH-1, -1)

if first_time:
    get_counters(PROCESS_DATE_COUNTERS, 2660000, blockCount+1, +1)
else:
    with open(address_rewards_run_file, 'r') as json_data:
        d = json.load(json_data)
        json_data.close()
        db_to_date_counters(ADDRESSES_POS, address_rewards_pos_cf)
        db_to_date_counters(ADDRESSES_MN, address_rewards_mn_cf)
        get_counters(PROCESS_DATE_COUNTERS, d["block"]+1, blockCount+1, +1)

try:
    for i in range (ADDRESSES_POS, ADDRESSES_MN+1):
        address_rewards_array  = []
        for k, v in block_counters[i].items():
            address_rewards_array.append({"a": k, "bc": v})
        address_rewards_array = sorted(address_rewards_array, key = lambda i: i["bc"][4], reverse=True)
        with open(address_rewards_block_counters_file[i], 'w+') as f:
            json.dump(address_rewards_array , f)

        address_rewards_array  = []
        for k, v in address_array[i].items():
            address_rewards_array.append({"a": k, "dc": v["dc"]})
        with open(address_rewards_date_counters_file[i], 'w+') as f:
            json.dump(address_rewards_array , f)

except:
    raise
finally:
    with open(address_rewards_run_file, 'w+') as f:
        address_rewards_run["block"] = blockCount
        json.dump(address_rewards_run, f)

httpConnection.close()
