import re
import logging
import datetime

DATE_FORMAT = "%Y%m%d"
DAY = 1

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def filter_generator(filters):
    for filter in filters:
        yield filter

def add_prefix(prefix: str, filters):
    """Add prefix to object name.

    Args:
        prefix(str): prefix
        filters(list, tuple): conditions of filtering object
    """
    for filter in filters:
        elements = filter.split("/")
        elements[len(elements) - 1] = "{0}_{1}". \
            format(prefix, elements[len(elements) - 1])
        yield '/'.join(elements)

def add_dateformat(dt: datetime.datetime, filters):
    """add date format to object name.
    
    Args:
        dt (datetime.datetime): datetime object
        filters (list, tuple): conditions of filtering object
    """
    recomp_date = re.compile("(%[a-zA-Z])+")
    for filter in filters:
        elements = filter.split("/")
        result = recomp_date.search(filter)
        if result:
            date_strf = dt.strftime(result.group(0))
            elements[len(elements) - 1] = elements[len(elements) - 1]. \
                replace(result.group(0), date_strf)
        yield '/'.join(elements)

# Get day 1day ago
def lambda_handler(event, context):
    filters = event['filter']
    
    now = datetime.datetime.now()
    target_date = now - datetime.timedelta(days=DAY)
    key_prefix = target_date.strftime(DATE_FORMAT)
    # generate new filters list
    newfilters = sorted(list(add_dateformat(target_date, filter_generator(filters))))

    return newfilters
