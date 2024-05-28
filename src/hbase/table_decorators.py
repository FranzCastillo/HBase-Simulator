from datetime import datetime
from functools import wraps


def update_timestamp(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self.metadata.updated_at = datetime.now()
        return func(self, *args, **kwargs)

    return wrapper
