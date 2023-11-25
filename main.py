from web3 import AsyncWeb3
import asyncio
import csv


class Web3AsyncWork:
    def __init__(self, blocks_needed: int, semaphore_value: int):
        self.blocks_needed = blocks_needed
        self.sem = asyncio.Semaphore(semaphore_value)
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider('https://api.zmok.io/mainnet/oaen6dy8ff6hju9k'))

    async def start(self, where_to_post):
        latest_block_number = await self.w3.eth.block_number
        tasks = []
        for i in range(latest_block_number, latest_block_number-self.blocks_needed, -1):
            tasks.append(asyncio.create_task(self.get_transactions_of_block(i, where_to_post)))

        await asyncio.gather(*tasks)

    async def get_transactions_of_block(self, current_block_number: int, where_to_post):
        async with self.sem:
            current_block = await self.w3.eth.get_block(current_block_number, full_transactions=True)
            for transaction in current_block['transactions']:
                info = [transaction['hash'].hex(), transaction['from'], transaction['to'], transaction['value'],
                        current_block['timestamp']]
                where_to_post.writerows([info])
            await asyncio.sleep(10)


fields = ['Hash', 'From', 'To', 'Value', 'Timestamp']
with open('transactions_report.csv', 'w') as transactions_report:
    write = csv.writer(transactions_report)
    write.writerow(fields)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Web3AsyncWork(blocks_needed=20, semaphore_value=10).start(where_to_post=write))
    loop.close()
