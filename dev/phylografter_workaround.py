#!/usr/bin/env python
import sys, json, codecs
from peyotl import write_as_json
inpfn = sys.argv[1]
outfn = sys.argv[2]
inp = codecs.open(inpfn, mode='rU', encoding='utf-8')
out = codecs.open(outfn, mode='w', encoding='utf-8')
obj = json.load(inp)
def rec_resource_meta(blob, k):
    if k == 'meta' and isinstance(blob, dict):
        if blob.get('@xsi:type') == 'nex:ResourceMeta':
            if (blob.get('@rel') is None):
                p = blob.get('@property')
                if p is not None:
                    del blob['@property']
                    blob['@rel'] = p
    if isinstance(blob, list):
        for i in blob:
            rec_resource_meta(i, k)
    else:
        for inner_k, v in blob.items():
            if isinstance(v, list) or isinstance(v, dict):
                rec_resource_meta(v, inner_k)

def coerce_boolean(blob, k):
    '''Booleans emitted as "true" or "false"
    for "@root" and "ot:isLeaf" meta
    '''
    if isinstance(blob, dict):
        if k == 'meta':
            if blob.get('@property') == 'ot:isLeaf':
                v = blob.get('$')
                try:
                    if v.lower() == "true":
                        blob['$'] = True
                    elif v.lower == "false":
                        blob['$'] = False
                except:
                    pass
        else:
            r = blob.get('@root')
            if r is not None:
                try:
                    if r.lower() == "true":
                        blob['@root'] = True
                    elif r.lower == "false":
                        blob['@root'] = False
                except:
                    pass
        for inner_k, v in blob.items():
            if isinstance(v, list) or isinstance(v, dict):
                coerce_boolean(v, inner_k)
    elif isinstance(blob, list):
        for i in blob:
            coerce_boolean(i, k)

def move_ott_taxon_name_to_otu(obj):
    nex = obj['nexml']
    ogl = nex['otus']
    tree_group_list = nex['trees']
    if not tree_group_list:
        return
    ogi_to_oid2otu = {}
    if not isinstance(ogl, list):
        ogl = [ogl]
    if not isinstance(tree_group_list, list):
        tree_group_list = [tree_group_list]
    for og in ogl:
        ogi = og['@id']
        od = {}
        for otu in og['otu']:
            oi = otu['@id']
            od[oi] = otu
        ogi_to_oid2otu[ogi] = od
    for tg in tree_group_list:
        ogi = tg['@otus']
        oid2otu = ogi_to_oid2otu[ogi]
        for tree in tg['tree']:
            for node in tree['node']:
                m = node.get('meta')
                if not m:
                    continue
                to_move = None
                if isinstance(m, dict):
                    if m.get('@property') == "ot:ottTaxonName":
                        to_move = m
                        del node['meta']
                else:
                    assert(isinstance(m, list))
                    ind_to_del = None
                    for n, ottnm in enumerate(m):
                        if ottnm.get('@property') == "ot:ottTaxonName":
                            to_move = ottnm
                            ind_to_del = n
                            break
                    if ind_to_del:
                        m.pop(n)
                if to_move:
                    oid = node['@otu']
                    otu = oid2otu[oid]
                    om = otu.get('meta')
                    if om is None:
                        otu['meta'] = to_move
                    elif isinstance(om, dict):
                        if om.get('@property') != "ot:ottTaxonName":
                            otu['meta'] = [om, to_move]
                    else:
                        assert(isinstance(om, list))
                        found = False
                        for omel in om:
                            if omel.get('@property') == "ot:ottTaxonName":
                                found = True
                                break
                        if not found:
                            om.append(to_move)
rec_resource_meta(obj, 'root')
coerce_boolean(obj, 'root')
move_ott_taxon_name_to_otu(obj)
write_as_json(obj, out)