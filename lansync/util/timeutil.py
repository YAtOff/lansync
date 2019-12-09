from datetime import datetime
import pytz


def now_as_iso():
    return datetime.now(tz=pytz.utc).isoformat()[:-6] + "000Z"
