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


def parse_time(dt):
    time_formats = ('%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S')
    for time_format in time_formats:
        try:
            return datetime.datetime.strptime(
                dt, time_format
            )
        except:
            pass
    raise Exception(f'Unknown timeformat for {dt}')


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def file_hash(file_path):
    import hashlib

    BLOCK_SIZE = 65536  # The size of each read from the file

    h = hashlib.sha256()  # Create the hash object, can use something other than `.sha256()` if you wish
    with open(file_path, 'rb') as f:  # Open the file to read it's bytes
        fb = f.read(BLOCK_SIZE)  # Read from the file. Take in the amount declared above
        while len(fb) > 0:  # While there is still data being read from the file
            h.update(fb)  # Update the hash
            fb = f.read(BLOCK_SIZE)  # Read the next block from the file

    return h.hexdigest()
