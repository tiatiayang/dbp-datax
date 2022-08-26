import time
def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        res = func(*args, **kw)
        print('current Function [%s] run time is %.2f' % (func.__name__, time.time() - local_time))
        #return func(*args, **kw)
        return res
    return wrapper


def _write_html(ipath, sql,type='w'):
    with open(ipath, type, encoding='utf-8') as fp:
        fp.write(sql)

def _read_html(ipath,type='r'):
    with open(ipath, type, encoding='utf-8') as fp:
        return fp.read()