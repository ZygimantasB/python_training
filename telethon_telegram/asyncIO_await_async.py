import time
import asyncio
from itertools import batched


async def brew_coffee():
    print('Start brew_coffee() ...')
    await asyncio.sleep(3)
    print('Stop brew_coffee()...')
    return 'Coffee ready'

async def toast_bagel():
    print('Start toast_bagel() ...')
    await asyncio.sleep(2)
    print('Stop toast_bagel()...')
    return 'Toast bagel'

async def main():
    start_time = time.time()

    # result_coffe = brew_coffee()
    # result_bagel = toast_bagel()

    # First approach

    # batch = asyncio.gather(brew_coffee(), toast_bagel())
    # result_coffe, result_bagel = await batch

    # second approach

    coffe_task = asyncio.create_task(brew_coffee())
    toast_bagel_task = asyncio.create_task(toast_bagel())

    result_coffe = await coffe_task
    result_bagel = await toast_bagel_task

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f'Result of brew coffee toast: {result_coffe}')
    print(f'Result of toast_bagel toast: {result_bagel}')
    print(f'Time elapsed: {elapsed_time}')

if __name__ == '__main__':
    asyncio.run(main())


