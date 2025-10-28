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

    batch = asyncio.gather(brew_coffee(), toast_bagel())
    result_coffe, result_bagel = await batch

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f'Result of brew coffee toast: {result_coffe}')
    print(f'Result of toast_bagel toast: {result_bagel}')
    print(f'Time elapsed: {elapsed_time}')

if __name__ == '__main__':
    asyncio.run(main())


