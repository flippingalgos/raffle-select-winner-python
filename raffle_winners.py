import time
import requests
import math
import numpy as np

from datetime import datetime
from algosdk import mnemonic
from algosdk.encoding import is_valid_address
from algosdk.error import WrongChecksumError
from algosdk.future.transaction import AssetTransferTxn
from algosdk.v2client import algod, indexer

NETWORK = "mainnet" #testnet
SENDER_ADDRESS = ""
SENDER_PASSPHRASE = ""  # 25 words separated by spaces

SLEEP_INTERVAL = 1  # AlgoExplorer limit for public calls
TRANSACTION_NOTE = "Flipping Algos Raffle Program - raffle.flippingalgos.xyz - Congradulations You WON!!"
GRAPHQL = "" #dev https://blue-surf-590599.us-east-1.aws.cloud.dgraph.io/graphql
DG_AUTH = ""
PS_AUTH =""

#TODO a check before we run that goes threw the wallets in the DB against the current blockchain results and confirm asset ids exists before sending to avoid over sending tokens

headers = {"DG-Auth": DG_AUTH}
query = """
{
  queryRaffles(filter: {isactive: true}) {
      id
      name
      image
      asset_id
      createdat
      lengthofraffle
      maxentries
      ticketcost
      rafflestransactions {
        id
        raffle_id
        receiver
        tokenunit
        txid
        createdat
        amountpaid
      }
      rafflestransactionsAggregate {
        count
      }
  }
}
"""

def run_query(query): # A simple function to use requests.post to make the API call. Note the json= section.
    request = requests.post(GRAPHQL, json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

def insert_query(address, asset_id, raffle_id, name, txid, createdat): # A simple function to use requests.post to make the API call. Note the json= section.
    insertquery = """
    mutation MyMutation($address: String!, $asset_id: Int!, $raffle_id: Int!,$name: String!,
    $txid: String!, $createdat: DateTime!) {
      addRaffles(input: {asset_id: $asset_id, name: 
      $name, wallet: {address: $address}, iscomplete: true, 
        raffleswinners: {receiver: $address, raffle_id: $raffle_id, asset_id: $asset_id, createdat: $createdat, txid: $txid} }, upsert: true) {
            numUids
        }
    }
    """
    variables = {'address': address, 'asset_id': asset_id, 'raffle_id': raffle_id, 'name': name, 'txid': txid, 'createdat': createdat }
    request = requests.post(GRAPHQL, json={'query': insertquery, 'variables': variables}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Insert failed to run by returning code of {}. {}".format(request.status_code, insertquery))

## CLIENTS
def _algod_client():
    """Instantiate and return Algod client object."""
    if NETWORK == "mainnet":
        algod_address = "https://mainnet-algorand.api.purestake.io/ps2"
    else:
        algod_address = "https://testnet-algorand.api.purestake.io/ps2"
    
    return algod.AlgodClient(
        PS_AUTH, algod_address, headers={"X-API-key": PS_AUTH}
    )


def _indexer_client():
    """Instantiate and return Indexer client object."""
    if NETWORK == "mainnet":
        indexer_address = "https://mainnet-algorand.api.purestake.io/idx2"
    else:
        indexer_address = "https://testnet-algorand.api.purestake.io/idx2"
    
    return indexer.IndexerClient(
        PS_AUTH, indexer_address, headers={"X-API-key": PS_AUTH}
    )


## TRANSACTIONS
def _wait_for_confirmation(client, transaction_id, timeout):
    """
    Wait until the transaction is confirmed or rejected, or until 'timeout'
    number of rounds have passed.
    Args:
        transaction_id (str): the transaction to wait for
        timeout (int): maximum number of rounds to wait
    Returns:
        dict: pending transaction information, or throws an error if the transaction
            is not confirmed or rejected in the next timeout rounds
    """
    start_round = client.status()["last-round"] + 1
    current_round = start_round

    while current_round < start_round + timeout:
        try:
            pending_txn = client.pending_transaction_info(transaction_id)
        except Exception:
            return
        if pending_txn.get("confirmed-round", 0) > 0:
            return pending_txn
        elif pending_txn["pool-error"]:
            raise Exception("pool error: {}".format(pending_txn["pool-error"]))
        client.status_after_block(current_round)
        current_round += 1
    raise Exception(
        "pending tx not found in timeout rounds, timeout value = : {}".format(timeout)
    )


def check_address(address, winnerasset):
    """Return True if address opted-in for the asset."""
    transactions = _indexer_client().search_transactions_by_address(
        address, asset_id=winnerasset
    )
    return True if len(transactions.get("transactions")) > 0 else False


def send_asset(receiver, giveawayamount, winnerasset, currenttxidcount):
    """Send asset to provided receiver address."""

    result = {}
    client = _algod_client()
    params = client.suggested_params()
    note = TRANSACTION_NOTE + " TXID Count: " + str(currenttxidcount)

    decimals = _algod_client().asset_info(winnerasset).get("params").get("decimals")
    amount = giveawayamount * (10 ** decimals)
    unsigned_txn = AssetTransferTxn(
        SENDER_ADDRESS,
        params,
        receiver,
        amount,
        index=winnerasset,
        note=note.encode(),
    )
    try:
        signed_txn = unsigned_txn.sign(mnemonic.to_private_key(SENDER_PASSPHRASE))
    except WrongChecksumError:
        return "Checksum failed to validate"
    except ValueError:
        return "Unknown word in passphrase"

    try:
        transaction_id = client.send_transaction(signed_txn)
        _wait_for_confirmation(client, transaction_id, 4)
    except Exception as err:
        result['response'] = "false"
        result['error'] = str(err)
        print(str(err))
        return result

    print(f"Amount of {giveawayamount} sent to {receiver} txid {transaction_id}")
    result['response'] = "true"
    result['transaction_id'] = transaction_id

    return result

if __name__ == "__main__":

    formatted_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    possiblewinners = []
    currenttxidcount = 0
    error_filename = "error_{}.txt".format(formatted_time)
    not_opted_in_filename = "not_opted_in_{}.txt".format(formatted_time)
    result = run_query(query) # Execute the query

    for raffle in result["data"]["queryRaffles"]:
        #print(raffle)
        txidcount = raffle["rafflestransactionsAggregate"]["count"]
        print("Raffle ID: " + str(raffle["id"]))
        print("Raffle Name: " + str(raffle["name"]))
        print("Asset ID Won: " + str(raffle["asset_id"]))
        print("Created At: " + str(raffle["createdat"]))
        print("TOTAL TICKET/TRANSACTION COUNT: " + str(txidcount))
        
        #build list of wallets to select a winner from
        #possiblewinners = [10, 20, 30, 40, 50, 20, 40]
        for transactions in raffle["rafflestransactions"]:
            possiblewinners.append(transactions["receiver"])

        #print("possiblewinners array", possiblewinners)
        winnerasset = raffle["asset_id"]
        winnerwallet = np.random.choice(possiblewinners, size=1)
        print("single random choice from 1-D array", winnerwallet)

        #this is for if we want multiple winners per raffle
        #items = np.random.choice(possiblewinners, size=3, replace=False)
        #print("multiple random choice from numpy 1-D array without replacement ", items)

        for winner in winnerwallet:
            if check_address(winner, winnerasset):
                time.sleep(SLEEP_INTERVAL)
                currenttxidcount += 1
                response = send_asset(winner, 1, winnerasset, currenttxidcount)
                #print(response)
                if response['response'] != "true":
                    with open(error_filename, "a") as error:
                        error.write(f"{response}\n")
                else:
                    #insert graphql transaction here from successful send
                    result = insert_query(winner, winnerasset, raffle['id'], raffle['name'], response['transaction_id'], str(datetime.now().isoformat())) # Execute the query
                    #print(result)
            else:
                with open(not_opted_in_filename, "a") as not_opted_in:
                    print("WALLET " + winner + " NOT OPT INTO ASSET ID " + str(winnerasset))
                    #not_opted_in.write(f"{winner}\n")
