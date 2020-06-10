import datetime
import json
import traceback
from pprint import pprint
import requests
from mysql import connector

with open("services.json") as file:
    config = json.load(file)
    sql_settings = config['sql_settings']
    httpprovider = config['httpprovider']

cnx = connector.connect(**sql_settings)

cursor = cnx.cursor(dictionary=True, buffered=True)
DEFAULT_BLOCK_REWARD = 40
DEFAULT_FEE = 100
GROTH_IN_BEAM = 100000000
WITHDRAWAL_FEE_PERC = 1


def get_unpaid_blocks():
    query = "SELECT * from blocks WHERE paid is NULL AND category = 'generate'"
    cursor.execute(query)
    records = cursor.fetchall()
    return records

def get_users_shares(time):
    try:
        users_shares = {}
        query = f"SELECT * FROM shares WHERE time >= {time} AND userid is not NULL"
        cursor.execute(query)
        records = cursor.fetchall()

        for _r in records:
            if str(_r['userid']) not in users_shares:
                users_shares[str(_r['userid'])] = 0
            users_shares[str(_r['userid'])] += _r['sharediff']

        return users_shares
    except Exception as exc:
        print(exc)


def get_users_portion(shares):
    total_shares = sum([float(shares[_x]) for _x in list(shares.keys())])
    portions = {}
    for _x in list(shares.keys()):
        user_portion = "{0:.2f}".format(float(shares[_x] / total_shares))
        reward_in_beams = "{0:.8f}".format(float(DEFAULT_BLOCK_REWARD * float(user_portion)))
        portions[_x] = {"portion": user_portion, "beams": reward_in_beams, "shares": shares[_x]}

    return portions, total_shares


def create_user_wallet():
    """
        Create new wallet address
    """
    response = requests.post(
        httpprovider,
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "create_address",
                "params":
                    {
                        "expiration": "never"
                    }
            })).json()

    print(response)
    return response

def get_txs_list(count=100, skip=0, filter={}):
    """
        Fetch list of txs
    """
    response = requests.post(
        httpprovider,
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tx_list",
                "params":
                    {
                        "filter": filter,
                        "skip": skip,
                        "count": count
                    }
            })).json()

    return response

def cancel_tx(tx_id):
    """
        Cancel Transaction
    """
    response = requests.post(
        httpprovider,
        data=json.dumps(
            {
                "jsonrpc":"2.0",
                "id": 4,
                "method": "tx_cancel",
                "params":
                {
                    "txId": tx_id
                }
            }
        )).json()

    print(response)
    return response


def send_transaction(
        value,
        fee,
        from_address,
        to_address,
        comment=""
):
    """
        Send transaction
    """
    try:
        response = requests.post(
            httpprovider,
            json.dumps({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tx_send",
                "params":
                    {
                        "value": value,
                        "fee": fee,
                        "from": from_address,
                        "address": to_address,
                        "comment": comment
                    }
            })).json()
        print(response)
        return response
    except Exception as exc:
        print(exc)


def update_balance():
    """
        Update user's balance using transactions history
    """
    print("Handle TXs")
    response = get_txs_list()

    for _tx in response['result']:
        try:

            if _tx['status'] == 1:
                check_hung_txs(tx=_tx)

            """
                Check withdraw txs    
            """
            cursor.execute(
                "SELECT * FROM txs WHERE txId = %s and receiver = %s",
                (_tx['txId'], _tx['receiver'],)
            )
            _is_tx_exist_withdraw = cursor.rowcount != 0

            cursor.execute(
                "SELECT * FROM payments WHERE to_address = %s AND txId = %s",
                (_tx['receiver'], _tx['txId'],)
            )
            _receiver = cursor.fetchone()

            if _receiver is not None and not _is_tx_exist_withdraw and \
                    (_tx['status'] == 4 or _tx['status'] == 3 or _tx['status'] == 2):

                value_in_beams = float((int(_tx['value']) + _tx['fee']) / GROTH_IN_BEAM)

                if _tx['status'] == 4:
                    cursor.execute(
                        f"INSERT INTO txs (txId,timestamp,sender,receiver,kernel,status,fee,value,comment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (_tx['txId'], _tx['create_time'], _tx['sender'], _tx['receiver'], "0000000000000000000000000000",
                         _tx['status'], _tx['fee'], _tx['value'], _tx['failure_reason'],)
                    )
                    cnx.commit()

                    cursor.execute(
                        "UPDATE payments SET status = %s, txId = %s WHERE to_address = %s AND txId = %s",
                        ("PENDING", None, _tx['receiver'],  _tx['txId'])
                    )
                    cnx.commit()
                    print(f"Tx {_tx['txId']}  {_tx['status_string']} to {_tx['receiver']}")

                elif _tx['status'] == 2:
                    print(_tx)
                    cursor.execute(
                        f"INSERT INTO txs (txId,timestamp,sender,receiver,kernel,status,fee,value,comment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (_tx['txId'], _tx['create_time'], _tx['sender'], _tx['receiver'], "0000000000000000000000000000", _tx['status'], _tx['fee'],
                         _tx['value'], _tx['comment'])
                    )
                    cnx.commit()

                    cursor.execute(
                        "UPDATE payments SET status = %s WHERE to_address = %s AND txId = %s",
                        ("CANCELLED", _tx['receiver'], _tx['txId'])
                    )
                    cnx.commit()
                    print(f"Tx {_tx['txId']}  {_tx['status_string']} to {_tx['receiver']}")

                else:
                    cursor.execute(
                        f"INSERT INTO txs (txId,timestamp,sender,receiver,kernel,status,fee,value,comment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (_tx['txId'], _tx['create_time'], _tx['sender'], _tx['receiver'], _tx['kernel'], _tx['status'], _tx['fee'],
                         _tx['value'], _tx['comment'])
                    )
                    cnx.commit()

                    cursor.execute(
                        "UPDATE payments SET status = %s WHERE to_address = %s AND txId = %s",
                        ("SENT_VERIFIED", _tx['receiver'], _tx['txId'])
                    )
                    cnx.commit()

                    print("Withdrawal Success\n"
                          "Balance of address %s has recharged on *%s* Beams." % (
                              _tx['sender'], value_in_beams
                          ))
                    print(f"Tx {_tx['txId']}  {_tx['status_string']} to {_tx['receiver']} | SENT Verified")
        except Exception as exc:
            print(exc)
            traceback.format_exc()

def check_hung_txs(tx):
    try:
        cancel_ts = int((datetime.datetime.now() - datetime.timedelta(minutes=720)).timestamp())
        if int(tx['create_time']) < cancel_ts:
            result = cancel_tx(tx_id=tx['txId'])
            print("Transaction %s cancelled\n%s" % (tx['txId'], result))
    except Exception as exc:
        print(exc)
        traceback.print_exc()

def create_table(q):
    try:
        cursor.execute(
            q
        )
    except connector.Error as err:
        print("Failed creating table: {}".format(err))


def update_tables_on_payment():

    unpaid_blocks = get_unpaid_blocks()
    for _x in unpaid_blocks:
        shares = get_users_shares(_x['time'])  # 2 - time
        portions, total_shares = get_users_portion(shares)
        pprint(portions)
        print("\nTotal Shares: ", total_shares)
        print(f"BLOCK #{_x['height']}")

        for account_id in portions.keys():
            cursor.execute(
                "SELECT * FROM accounts WHERE id = %s",
                (account_id,)
            )
            _account = cursor.fetchone()
            reward_in_beams = float(portions[account_id]['beams'])
            reward_in_groth = int(reward_in_beams * GROTH_IN_BEAM)
            withdrawal_fee_groth = int(reward_in_groth * (WITHDRAWAL_FEE_PERC / 100))
            reward_in_groth = reward_in_groth - withdrawal_fee_groth - DEFAULT_FEE
            timestamp = int(datetime.datetime.now().timestamp())
            block_height = int(_x['height'])


            if reward_in_beams < 0:
                continue

            cursor.execute(
                f"INSERT INTO payments (to_address, timestamp, txId, status, fee, withdrawal_fee, value, block_height) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (_account['username'], timestamp, None, "PENDING", DEFAULT_FEE, withdrawal_fee_groth, reward_in_groth, block_height)
            )
            cnx.commit()
        print("UPDATE blocks SET paid = %s, paid_at = %s WHERE height = %s" %
            (True, timestamp, block_height))
        cursor.execute(
            "UPDATE blocks SET paid = %s, paid_at = %s WHERE height = %s",
            (True, timestamp, block_height)
        )
        cnx.commit()


def payment_processing():
    try:
        query = f"SELECT * FROM payments WHERE status = 'PENDING' AND txId is NULL"
        cursor.execute(query)
        records = cursor.fetchall()
        for _x in records:
            result = send_transaction(
                value=_x['value'],
                fee=_x['fee'],
                from_address=FROM_ADDRESS,
                to_address=_x['to_address']
            )
            cursor.execute(
                "UPDATE payments SET status = 'SENT', txId = %s WHERE block_height = %s AND to_address = %s",
                (result['result']['txId'], _x['block_height'], _x['to_address'])
            )
            print(f"Status of block #{_x['block_height']} and address {_x['to_address']}")
            cnx.commit()
    except Exception as exc:
        print(exc)


FROM_ADDRESS = create_user_wallet()['result']
# FROM_ADDRESS = "newwaddressgenerated"
create_table(
"""
CREATE TABLE IF NOT EXISTS `txs` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `txId` varchar(64) DEFAULT NULL,
 `timestamp` int DEFAULT NULL,
 `sender` varchar(255) DEFAULT NULL,
 `receiver` varchar(255) DEFAULT NULL,
 `kernel` varchar(255) DEFAULT NULL,
 `status` varchar(64) DEFAULT NULL,
 `fee` bigint DEFAULT NULL,
 `value` bigint DEFAULT NULL,
 `comment` varchar(255) DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4;
""")

create_table(
"""
CREATE TABLE IF NOT EXISTS `payments` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `to_address` varchar(255) DEFAULT NULL,
 `block_height` int DEFAULT NULL,
 `timestamp` int DEFAULT NULL,
 `txId` varchar(64) DEFAULT NULL,
 `status` varchar(64) DEFAULT NULL,
 `fee` bigint DEFAULT NULL,
 `withdrawal_fee` bigint DEFAULT NULL,
 `value` bigint DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4;
""")

update_balance()
update_tables_on_payment()
payment_processing()

cursor.close()
cnx.close()
