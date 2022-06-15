import os
import fnmatch

def _value_map(f, kv_pair_iter):
    for k, v in kv_pair_iter:
        yield k, f(v)

def value_map(f, kv_pair_iter, as_dict=False):
    it = _value_map(f, kv_pair_iter)
    if as_dict:
        return dict(it)
    else:
        return it

def list_files(d, glob_string=None):
    """List of all files under root d matching glob_string"""
    def _list():
        for root, dirs, files in os.walk(d):
            if glob_string is None:
                filtered_files = files
            else:
                filtered_files = fnmatch.filter(files, glob_string)
            yield from (os.path.join(root, f) for f in filtered_files)

    return (list(_list()))
