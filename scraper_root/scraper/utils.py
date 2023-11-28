import datetime
import logging

logger = logging.getLogger(__name__)


def from_timestamp(timestamp: int) -> datetime.datetime:
    if timestamp > 2000000000:
        return datetime.datetime.fromtimestamp(int(timestamp / 1000), datetime.timezone.utc)
    else:
        return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def readable(timestamp: int):
    if timestamp is None:
        return 'None'
    try:
        human_readable = from_timestamp(timestamp)
    except TypeError:
        return 'None'
    except Exception:
        logger.exception(f'Failed to convert timestamp {timestamp} to readable form')
        return f'{timestamp} (CHECK EXCEPTION!)'
    return f'{human_readable} ({timestamp})'
