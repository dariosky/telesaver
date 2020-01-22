import datetime
import string
import unicodedata


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """

    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)

    cleaned_value = unicodedata.normalize('NFKD', value).encode('ASCII', 'ignore')
    return ''.join(chr(c) for c in cleaned_value if chr(c) in valid_chars)


def parse(dt, TIME_FORMAT):
    return datetime.datetime.strptime(
        dt, TIME_FORMAT
    )


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
