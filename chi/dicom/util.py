def _value_map(f, kv_pair_iter):
    for k, v in kv_pair_iter:
        yield k, f(v)

def value_map(f, kv_pair_iter, as_dict=False):
    it = _value_map(f, kv_pair_iter)
    if as_dict:
        return dict(it)
    else:
        return it

