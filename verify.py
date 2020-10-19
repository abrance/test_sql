
"""
md5 verify
"""

from hashlib import md5


class MD5ParamException(Exception):
    def __str__(self):
        return 'md5 param error, text must be str or bytes'


class MD5(object):

    """
    MD5 校验工具类
    """
    @staticmethod
    def get(text):
        if isinstance(text, str):
            text = text.encode()
        elif isinstance(text, bytes):
            pass
        else:
            raise Exception
        m = md5()
        m.update(text)
        return m.hexdigest()


def get_md5(text: (str, bytes), obj=MD5):
    """
    factory
    :param text: md5(text)
    :param obj: implement get()
    :return:
    """
    if getattr(obj, 'get'):
        pass
    else:
        return False

    return obj.get(text)
