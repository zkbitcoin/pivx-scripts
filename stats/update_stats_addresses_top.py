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
                    default="../../../../stats_data/address-top/json",
                    dest="oloc",
                    metavar="<output-location-path>",
                    help="output-location-path")
parser.add_argument('-d', '--db-location-path',
                    required=False,
                    type=str,
                    default="../../../../stats_data/address-top/db",
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

address_balances_top_run_file = os.path.join(args.rloc, "address_top_run.json")
address_balances_top_file = os.path.join(args.oloc, "address_top.json")

THREADS_COUNT=5

class CliClient:

    def __init__(self):
        self.url = args.apiurl

    def checkResponse(self, method, param=""):
        url = self.url + "/api/v2/%s" % method
        if param != "":
            url += "/%s" % param
        url += "?details=basic"
        s = requests.Session()
        retries = Retry(total=10, backoff_factor=5, status_forcelist=[ 502, 503, 504 ])
        if args.apiurl.startswith('https'):
            s.mount('http://', HTTPAdapter(max_retries=retries))
            resp = requests.get(url, data={}, verify=False, cert=(args.crt, args.key))
        else:
            s.mount('https://', HTTPAdapter(max_retries=retries))
            resp = requests.get(url, data={}, verify=True)
        s.close()
        if resp.status_code == 200:
            data = resp.json()
            return data
        #cold staking contract with no balance https://zkbitcoin.com/block/2155851
        # address: SckTegQMthCoxj2Q1BHKvkvhs9m4Cm5RYy
        if resp.status_code == 400:
            return 0
        raise Exception("Invalid response")

    def getAddress(self, address: object) -> object:
        return self.checkResponse("address", address)

class ApiClient:
    def updatesorted(self, address):
        #print("Calling API for address %s" % (address))
        try :
            cliClient = CliClient()
            address_api = cliClient.getAddress(address)
            if address_api == 0:
                return
            balance = int(address_api["balance"])
            if balance > 10000:
                address_data = {}
                address_data["address"] = address
                address_data["balance"] = balance
                address_data["totalReceived"] = int(address_api["totalReceived"])
                address_data["totalSent"] = int(address_api["totalSent"])
                address_data["unconfirmedBalance"] = int(address_api["unconfirmedBalance"])
                address_data["txs"] = int(address_api["txs"])
                #print("Updating address %s %s" % (address, address_data["txs"]))
                db.put((address_stats_cf, bytes(address, 'utf-8')),bytes(json.dumps(address_data), 'utf-8'))
        except Exception as e:
            raise
apiClient = ApiClient()

options = rocksdb.Options()
options.create_if_missing = True

first_time = False

try:
    with open(address_balances_top_run_file, 'r') as f:
        address_balance_run = json.load(f)
except FileNotFoundError:
    # first run (hardcoded)
    print("First run...")
    first_time = True
    db = rocksdb.DB(args.dbloc, options, column_families={}, read_only=False)
    db.create_column_family(b'address_stats_cf', rocksdb.ColumnFamilyOptions())
    del db
    address_balance_run = {}
    address_balance_run["block"] = 0

column_families = {
    b'default': rocksdb.ColumnFamilyOptions(),  # default CF is already exists, so this is redundant
    b'address_stats_cf': rocksdb.ColumnFamilyOptions(),
}
db = rocksdb.DB(args.dbloc, options, column_families=column_families, read_only=False)
address_stats_cf = db.get_column_family(b'address_stats_cf')

# Initialize RPC connection and API client
httpConnection = httplib.HTTPConnection(rpc_host, rpc_port, timeout=5)
conn = AuthServiceProxy(rpc_url, timeout=30, connection=httpConnection)

blockCount = conn.getblockcount()

retry_blocks = {}
retry_addresses = {}

processed_total = 0


def process(thread_name, start_block, end_block):
    for i in range (start_block, end_block):
        global processed_total
        processed_total = processed_total + 1
        #if processed_total % 1000 == 0:
        print("total processed %i..." % (processed_total))
        #print("Thread %s Getting block %d..." % (thread_name, i))
        block = {}
        for j in range(0,10):
            try:
                block_hash = conn.getblockhash(i)
                block = conn.getblock(block_hash, True)
                break
            except Exception as e:
                #print(e)
                time.sleep(5)
        if len(block) == 0:
            print('%s Error: timed out on block: %i retries: %i' % (thread_name, i, j))
            retry_blocks[i] = i
            continue
        for tx_hash in block['tx']:
            for j in range(0,10):
                try:
                    tx_raw = conn.getrawtransaction(tx_hash, True)
                    break
                except Exception as e:
                    #print(e)
                    time.sleep(5)
            if len(tx_raw) == 0:
                print('%s Error: timed out on block: %i retries: %i' % (thread_name, i, j))
                retry_blocks[i] = i
                break
            for vout in tx_raw['vout']:
                if "addresses" in vout["scriptPubKey"]:
                    tx_addresses = vout["scriptPubKey"]["addresses"]
                    for address in tx_addresses:
                        #if db.get((address_stats_cf, bytes(address, 'utf-8'))) is not None:
                        #    continue
                        #db.put((address_stats_cf, bytes(address, 'utf-8')), b'')
                        try:
                            apiClient.updatesorted(address)
                        except Exception as e:
                            print('%s Error: timed out on address: %s' % (thread_name, address))
                            retry_addresses[address] = address

class ProcessThread(threading.Thread):
    def run(self):
        start_block = self._args["start_block"]
        end_block = self._args["end_block"]
        print("{} processing from block: {} to block: {}".format(self.getName(), start_block, end_block))
        process(self.getName(), start_block, end_block)


def main():

    thread_array = []

    l = (blockCount - address_balance_run["block"])
    e = int(l / THREADS_COUNT)

    beg = address_balance_run["block"] + 1

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

    for block in retry_blocks:
        print("retrying block %i" % block)
        process("retry blocks: ", block, block+1)

    for address in retry_addresses:
        print("retrying address %s" % address)
        apiClient.updatesorted(address)

    address_balance_run["block"] = blockCount

    try:
        address_balance_array = []
        address_balance_hash = {}

        it = db.iteritems(address_stats_cf)
        it.seek_to_first()
        j = 0
        for i in it:
            j +=1
            #print("Getting block for sort %d..." % j)
            k = str(i[0][1], "utf-8")
            v = str(i[1], "utf-8")
            if v == '':
                continue
            v = json.loads(str(i[1], "utf-8"))
            address_balance = {}
            address_balance["address"] = k
            address_balance["balance"] = v["balance"]
            address_balance["totalReceived"] = v["totalReceived"]
            address_balance["totalSent"] = v["totalSent"]
            address_balance["unconfirmedBalance"] = v["unconfirmedBalance"]
            address_balance["txs"] = v["txs"]
            address_balance_array.append(address_balance)
            address_balance_hash[k] = True

        address_balance_array = sorted(address_balance_array, key = lambda i: i["balance"], reverse=True)

        address_balance_sorted_array = []
        for i in range(len(address_balance_array)):
            if i > 100:
                break;
            address_balance_sorted_array.append(address_balance_array[i])

        it = db.iterkeys(address_stats_cf)
        it.seek_to_first()
        for i in it:
            k = i[1]
            if str(k, 'utf-8') not in address_balance_hash:
        #       print("Deleting address %s..." % str(k, 'utf-8'))
                db.delete((address_stats_cf, k))
        #   else:
        #       print("Found address %s..." % str(k, 'utf-8'))

        with open(address_balances_top_file, 'w+') as f:
            json.dump(address_balance_sorted_array, f)

    except Exception as e:
        raise
    finally:
        with open(address_balances_top_run_file, 'w+') as f:
            address_balance_run["block"]
            json.dump(address_balance_run, f)

    httpConnection.close()

if __name__ == "__main__":
    main()
