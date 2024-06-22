import hmac, base64
import asyncio, aiohttp
import datetime
import json
from pathlib import Path
import csv

from config import OKX_SUBACCOUNTS_API_KEYS, TOKEN, CHAIN

class OKX:

    def __init__(self, api_key, api_secret, passphras):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphras = passphras

    async def make_http_request(self, url, method="GET", headers=None, params=None, data=None, timeout=10):
        async with aiohttp.ClientSession() as session:
            kwargs = {"url": url, "method": method, "timeout": timeout}
            
            if headers:
                kwargs["headers"] = headers
            
            if params:
                kwargs["params"] = params
            
            if data:
                kwargs["data"] = data
            
            async with session.request(**kwargs) as response:
                return await response.json()

    async def get_data(self, request_path="/api/v5/account/balance?ccy=USDT", body='', meth="GET"):

        def signature(
            timestamp: str, method: str, request_path: str, secret_key: str, body: str = ""
        ) -> str:
            if not body:
                body = ""

            message = timestamp + method.upper() + request_path + body
            mac = hmac.new(
                bytes(secret_key, encoding="utf-8"),
                bytes(message, encoding="utf-8"),
                digestmod="sha256",
            )
            d = mac.digest()
            return base64.b64encode(d).decode("utf-8")

        dt_now = datetime.datetime.utcnow()
        ms = str(dt_now.microsecond).zfill(6)[:3]
        timestamp = f"{dt_now:%Y-%m-%dT%H:%M:%S}.{ms}Z"

        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature(timestamp, meth, request_path, self.api_secret, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphras,
            'x-simulated-trading': '0'
        }

        return headers

    async def get_deposit_address(self, acc: str, currency: str, _chain: str):
        try:
            headers = await self.get_data(request_path=f"/api/v5/asset/deposit-address?ccy={currency}", meth="GET")
            result = await self.make_http_request(f"https://www.okx.cab/api/v5/asset/deposit-address?ccy={currency}", method="GET", headers=headers)
            if result["msg"] != "":
                print(f'{acc} | {result["msg"]}')
                return None, None

            chains = []
            addresses = []
            for data in result["data"]:
                chain = data["chain"].split("-")[1]
                if chain not in chains:
                    chains.append(chain)
                if chain.lower() == _chain.lower():
                    addresses.append(data["addr"])
                
            return addresses, chains
        except Exception as error:
            print(f'{acc} | error: {error}')
            return None, None

def call_json(result: list | dict, filepath: Path | str):
    with open(f"{filepath}.json", "w") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

async def fetch_addresses():
    chains = []
    results = {}
    for acc, data in OKX_SUBACCOUNTS_API_KEYS.items():
        okx = OKX(api_key=data["api_key"], api_secret=data["api_secret"], passphras=data["passphras"])
        addresses, chains = await okx.get_deposit_address(acc, TOKEN, CHAIN)
        if addresses:
            results[acc] = addresses
    return results, chains

def save_to_csv(data, filepath):
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["subaccount", "address"])
        for acc, addresses in data.items():
            print(f"{acc} | {len(addresses)} addresses")
            for address in addresses:
                writer.writerow([acc, address])


if __name__ == "__main__":
    print("\nStart")
    results, chains = asyncio.run(fetch_addresses())
    if results:
        # call_json(chains, "chains")
        call_json(results, "deposit_addresses")
        save_to_csv(results, 'deposit_addresses.csv')
        print("Results are recorded in deposit_addresses\n")
