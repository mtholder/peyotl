#!/usr/bin/env python

def new_diff_summary():
    d = {}
    _add_diff_fields(d)
    return d

OT_DIFF_TYPE_LIST = ('rerootings',
                     'deletions',
                     'additions',
                     'modifications',
                     'key-ordering')
def _add_diff_fields(d):
    for k in OT_DIFF_TYPE_LIST:
        if k not in d:
            d[k] = []
    return d

def extract_by_diff_type(d, diff_obj):
    from peyotl.nexson_diff.nexson_diff import RerootingDiff, \
                                           DeletionDiff, \
                                           NoModDelDiff, \
                                           AdditionDiff, \
                                           ByIdAddDiff, \
                                           NoModAddDiff, \
                                           ModificationDiff, \
                                           SetAddDiff, \
                                           SetDelDiff, \
                                           ByIdDelDiff, \
                                           ElementOrderDiff

    if isinstance(diff_obj, RerootingDiff):
        return d['rerootings']
    elif isinstance(diff_obj, AdditionDiff):
        return d['additions']
    elif isinstance(diff_obj, DeletionDiff):
        return d['deletions']
    elif isinstance(diff_obj, ModificationDiff):
        return d['modifications']
    elif isinstance(diff_obj, ElementOrderDiff):
        return d['key-ordering']
    else:
        raise NotImplementedError('Unknown Diff Type')

def to_ot_diff_dict(native_diff):
    r = {}
    for dt in OT_DIFF_TYPE_LIST:
        kvc_list = native_diff.get(dt, [])
        x = []
        for diff_obj in kvc_list:
            y = diff_obj.as_ot_diff()
            x.append(y)
        if kvc_list:
            r[dt] = x
    return r

_KNOWN_ORDERED_KEYS = {'treesById':('treesById', True),
                       'trees': (None, False),
                       'otus': (None, False)}


def dict_patch_modified_blob(base_blob, diff_dict, patch_log):
    for d_type in OT_DIFF_TYPE_LIST:
        d_list = diff_dict.get(d_type)
        for d_obj in d_list:
            d_obj.patch_mod_blob(base_blob, patch_log)

def _ordering_to_ot_diff(od):
    r = {}
    for k, v in od.items():
        if k == 'address':
            x = v.as_ot_target()
            r.update(x)
        else:
            if isinstance(v, set):
                r[k] = list(v)
            else:
                r[k] = v
    return r

def _dict_of_ordering_to_ot_diff(od):
    #_LOG.debug(od.keys())
    nested_key = 'treesById'
    r = {}
    for k, v in od.items():
        if k != nested_key:
            r[k] = _ordering_to_ot_diff(v)
        else:
            n = {}
            r[nested_key] = n
            for k2, v2 in v.items():
                n[k2] = _ordering_to_ot_diff(v2)
    return r


def detect_no_mod_list_dels_adds(src, dest):
    '''Takes an ancestor (src),  descendant (dest) pair of lists
    and returns a pair of lists:
        dels = the objects in src for which there is not object in dest that compares equal to
        adds = the objects in dest for which there is not object in src that compares equal to
    '''
    dfound_set = set()
    sunfound_set = set()
    dels, adds = [], []
    for sn, el in enumerate(src):
        found = False
        for dn, d_el in enumerate(dest):
            if el == d_el:
                dfound_set.add(dn)
                found = True
                break
        if not found:
            dels.append(el)
            sunfound_set.add(sn)
    for dn, d_el in enumerate(dest):
        if dn not in dfound_set:
            found = False
            for sn, el in enumerate(src):
                if sn not in sunfound_set:
                    if el == d_el:
                        found = True
            if not found:
                adds.append(d_el)
    return dels, adds