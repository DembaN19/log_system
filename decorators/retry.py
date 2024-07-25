import time
from functools import wraps
from typing import Callable, Any
from time import sleep
import logging as logger


def retry_functions(retries: int = 3, delay: float = 1) -> Callable:
    """
    Attempt to call a function, if it fails, try again with a specified delay.

    :param retries: The max amount of retries you want for the function call
    :param delay: The delay (in seconds) between each function retry
    :return:
    """

    # Check invalid input from the user
    if retries < 1 or delay <= 0:
        raise ValueError('Please use the appropriate values?')

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for i in range(1, retries + 1):  # 1 to retries + 1 since upper bound is exclusive

                try:
                    logger.info(f'Running ({i}): {func.__name__}()')
                    return func(*args, **kwargs)
                except Exception as e:
                    # Break out of the loop if the max amount of retries is exceeded
                    if i == retries:
                        logger.error(f'Error: {repr(e)}.')
                        logger.error(f'"{func.__name__}()" failed after {retries} retries.')
                        break
                    else:
                        logger.error(f'Error: {repr(e)} -> Retrying...')
                        sleep(delay)  # Add a delay before running the next iteration

        return wrapper

    return decorator


# Example of use
@retry_functions(retries=3, delay=1) # 1 usage
def connect() -> None:
    time.sleep(1)
    raise Exception('Could not connect to internet...')


def main() -> None:
    connect()


if __name__ == '__main__':
    main()