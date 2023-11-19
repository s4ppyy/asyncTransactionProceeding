from web3 import AsyncWeb3
import asyncio
import csv


class Web3AsyncWork:
    def __init__(self, blocks_needed):
        self.transactions_list = list()
        self.blocks_needed = blocks_needed
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider('https://api.zmok.io/mainnet/oaen6dy8ff6hju9k'))

    async def start(self):
        sem = asyncio.Semaphore(10)
        latest_block_number = await self.w3.eth.block_number
        tasks = []
        for i in range(latest_block_number, latest_block_number-self.blocks_needed, -10):
            for j in range(i, i - 10, -1):
                task = asyncio.create_task(self.get_transactions_of_block(current_block_number=j, sem=sem))
                tasks.append(task)

            await asyncio.gather(*tasks)
            tasks.clear()
            # await asyncio.sleep(60)


        # await asyncio.gather(*tasks, return_exceptions=False)
        return self.transactions_list

    async def get_transactions_of_block(self, current_block_number, sem):
        async with sem:
            current_block = await self.w3.eth.get_block(current_block_number, full_transactions=True)
            # self.transactions_list.append(current_block['transactions'])
            for transaction in current_block['transactions']:
                info = [transaction['hash'], transaction['from'], transaction['to'], transaction['value'],
                        current_block['timestamp']]
                self.transactions_list.append(info)


test = Web3AsyncWork(blocks_needed=20)
res = asyncio.run(test.start())
print(res)
fields = ['Hash', 'From', 'To', 'Value', 'Timestamp']
with open('transactions_report.csv', 'w') as transactions_report:
    write = csv.writer(transactions_report)
    write.writerow(fields)
    write.writerows(res)
