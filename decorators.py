import time


def measure_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        print(f"함수 {func.__name__}의 실행 시간: {elapsed:.3f}초")
        return result

    return wrapper
