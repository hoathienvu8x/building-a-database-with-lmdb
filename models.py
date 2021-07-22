from ctypes import Structure, Array, c_double, c_char
from datetime import datetime
from uuid import UUID


class AsDictMixin:
    @property
    def as_dict(self):
        d = {}
        for (key, _) in self._fields_:
            if isinstance(getattr(self, key), AsDictMixin):
                d[key] = getattr(self, key).as_dict
            elif isinstance(getattr(self, key), bytes):
                d[key] = getattr(self, key).decode()
            else:
                d[key] = getattr(self, key)
        return d


class Ident(Array):
    _length_ = 36
    _type_ = c_char


class Sample(Structure, AsDictMixin):
    _fields_ = [("ident", Ident), ("sample", c_double), ("timestamp", c_double)]

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(['='.join([key, str(val)]) for key, val in self.as_dict.items()])})"

    def is_valid(self):
        return (
            valid_sample(self.sample)
            or valid_timestamp(self.timestamp)
            or valid_ident(self.ident)
        )


def valid_timestamp(timestamp: float):
    return timestamp <= (datetime.now().timestamp() - 1)


def valid_sample(sample: float):
    return 0 < sample <= 1


def valid_ident(ident: bytes):
    try:
        str_ident = ident.decode("utf-8")
    except UnicodeDecodeError:
        return False
    try:
        UUID(str_ident)
    except ValueError:
        return False
    return True
