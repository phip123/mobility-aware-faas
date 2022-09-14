# The following code was taken from edgerun/skippy-core
# https://github.com/edgerun/skippy-core/blob/09f9cbbee7f8c39d93468ef7cd9b6c26a69fd073/skippy/core/utils.py#L24
import re

__size_conversions = {
    'K': 10 ** 3,
    'M': 10 ** 6,
    'G': 10 ** 9,
    'T': 10 ** 12,
    'P': 10 ** 15,
    'E': 10 ** 18,
    'Ki': 2 ** 10,
    'Mi': 2 ** 20,
    'Gi': 2 ** 30,
    'Ti': 2 ** 40,
    'Pi': 2 ** 50,
    'Ei': 2 ** 60
}

__size_pattern = re.compile(r"([0-9]+)([a-zA-Z]*)")


def parse_size_string_to_bytes(size_string: str) -> int:
    size_string = size_string.replace('"', '')
    m = __size_pattern.match(size_string)
    if not m:
        raise ValueError('invalid size string: %s' % size_string)

    if len(m.groups()) > 1:
        number = m.group(1)
        unit = m.group(2)
        return int(number) * __size_conversions.get(unit, 1)
    else:
        return int(m.group(1))
