import time


def measure_time(func):
    def wrapper(*args, **kwargs):
        owner = kwargs.get('owner') or args[0]
        table_name = kwargs.get('table_name') or args[1]
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        print(f"{owner}.{table_name} {func.__name__} 실행 시간: {elapsed:.3f}초")
        return result
    return wrapper
