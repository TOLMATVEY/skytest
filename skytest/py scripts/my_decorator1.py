#!usr/bin/python3
import time
from functools import wraps
import logging


# Функция для расчета общего количества памяти
# from decimal import Decimal

# def process_memory():
#     process = psutil.Process(os.getpid())
#     mem_info = process.memory_info()
#     return mem_info.rss

# декоратор для расчета затраченного времени и памяти
def decorator_time_mem(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        result = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # # Measure memory
        # mem_before = process_memory()
        # result = fn(*args, **kwargs)
        # mem_after = process_memory()
        # print("{}:consumed memory: {:,}".format(
            # fn.__name__,
            # mem_before, mem_after, mem_after - mem_before))
        return result
    return inner

# Декоратор для обеспечения ретрая (мало-ли, что-то пойдет не так)
# Работает просто, пишишь количество ретраев, задержку между попытками
# и все у нас замечатольно.
def retry(max_tries = 5, delay_seconds = 5):
    def decorat_retry(func):
        @wraps(func)
        def inner(*args, **kwargs):
            tries = 0
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        raise e
                    time.sleep(delay_seconds)
        return inner
    return decorat_retry

# Функции кеширования. Если аргументы наших функций не меняются,
#  эта функция позволит ускорить выполнение функции.
def memorys(func):
    cache = {}
    def inner(*args):
        if args in cache:
            return cache[args]
        else:
            result = func(*args)
            cache[args] = result
            return result
    return inner


# Функция логирования 
# Это я уже после допер, когда ту "муру" написал
logging.basicConfig(level=logging.INFO)

def log_exec(func):
    @wraps(func)
    def inner(*args, **kwargs):
        logging.info(f"Executing {func.__name__}")
        result = func(*args, **kwargs)
        logging.info(f"Finished executing {func.__name__}")
        return result
    return inner

