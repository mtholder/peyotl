#!/usr/bin/env python
from peyotl.collections_store import get_empty_collection
from peyotl import (collection_to_included_trees,
                    read_as_json,
                    write_as_json)
from peyotl.phylesystem import Phylesystem
from collections import defaultdict
import filecmp
import shutil
import codecs
import copy
import sys
import os

# for newick_to_nexson
from peyotl.phylo.tree import parse_newick
from peyotl.nexson_syntax import get_empty_nexson
import re

_VERBOSE = False
_SCRIPT_NAME = 'export_studies_from_collection.py'


def debug(msg):
    if _VERBOSE:
        sys.stderr.write('{}: {}\n'.format(_SCRIPT_NAME, msg))


def error(msg):
    sys.stderr.write('{}: Error: {}\n'.format(_SCRIPT_NAME, msg))


def copy_phylesystem_file_if_differing(git_action,
                                       sha,
                                       coll_decision,
                                       out_dir,
                                       cd_to_new_map):
    study_id = coll_decision['studyID']
    tree_id = coll_decision['treeID']
    fp = git_action.path_for_doc(study_id)
    new_name = '{}@{}.json'.format(study_id, tree_id)
    np = os.path.join(out_dir, new_name)
    if not os.path.isfile(fp):
        debug(fp + ' does not exist')
        error('Study file "{}" does not exist. Assuming that this study has been deleted'.format(fp))
        if os.path.isfile(np):
            os.remove(np)
        return False
    # create a new "decision" entry that is bound to this SHA
    concrete_coll_decision = copy.deepcopy(coll_decision)
    concrete_coll_decision['SHA'] = sha
    cd_to_new_map[id(coll_decision)] = concrete_coll_decision
    # copy the file, if necessary
    if (not os.path.exists(np)) or (not filecmp.cmp(fp, np)):
        debug('cp "{}" "{}"'.format(fp, np))
        shutil.copy(fp, np)
        return True
    debug('"{}" and "{}" are identical'.format(fp, np))
    return False



_ID_EXTRACTOR = re.compile(r'^.*[^0-9]([0-9]+)$')
_ALL_DIGIT_ID_EXTRACTOR = re.compile(r'^([0-9]+)$')


def ott_id_from_label(s):
    x = _ID_EXTRACTOR.match(s)
    if not x:
        x = _ALL_DIGIT_ID_EXTRACTOR.match(s)
        if not x:
            raise RuntimeError('Expecting each tip label to end in an integer. Found "{}"'.format(s))
    return int(x.group(1))


def newick_to_nexson(inp, tree_id, ottid_from_label):
        pyid2int = {}
        curr_nd_counter = 1
        tree = parse_newick(stream=inp)
        nexson = get_empty_nexson()
        body = nexson['nexml']
        all_otus_groups = body['otusById'].values()
        assert len(all_otus_groups) == 1
        first_otus_group = all_otus_groups[0]
        all_trees_groups = body['treesById'].values()
        assert len(all_trees_groups) == 1
        first_trees_group = all_trees_groups[0]
        first_trees_group['^ot:treeElementOrder'].append(tree_id)
        otus = first_otus_group['otuById']
        all_trees_dict = first_trees_group['treeById']
        ntree = all_trees_dict.setdefault(tree_id, {})
        ebsi, nbi = {}, {}
        ntree['edgeBySourceId'] = ebsi
        ntree['nodeById'] = nbi
        root_node_id = None
        for node in tree._root.preorder_iter():
            nid = id(node)
            i = pyid2int.get(nid)
            if i is None:
                i = curr_nd_counter
                curr_nd_counter += 1
                pyid2int[nid] = i
            node_id_s = 'node{}'.format(i)
            otu_id_s = 'otu{}'.format(i)
            n_obj = nbi.setdefault(node_id_s, {})
            if node is tree._root:
                n_obj['@root'] = True
                root_node_id = node_id_s
            else:
                edge_id_s = 'edge{}'.format(i)
                pid = id(node.parent)
                pni = 'node{}'.format(pyid2int[pid])
                ed = ebsi.setdefault(pni, {})
                ed[edge_id_s] = {'@source': pni, '@target': node_id_s}
            if not node.children:
                n_obj['@otu'] = otu_id_s
                orig = node._id
                ott_id = ott_id_from_label(orig)
                otus[otu_id_s] = {"^ot:originalLabel": orig, "^ot:ottId": ott_id, "^ot:ottTaxonName": orig}
        assert root_node_id is not None
        ntree['^ot:rootNodeId'] = root_node_id
        return nexson

if __name__ == '__main__':
    import argparse

    description = 'Takes an collection JSON and prints out information from it'
    parser = argparse.ArgumentParser(prog=_SCRIPT_NAME, description=description)
    parser.add_argument('--phylesystem-par',
                        default=None,
                        type=str,
                        required=False,
                        help='directory that holds the phylesystem shards (optional if you have peyotl configured)')
    parser.add_argument('collection',
                        default=None,
                        type=str,
                        help='filepath for the collections JSON')
    parser.add_argument('--output-dir',
                        default=None,
                        type=str,
                        required='False',
                        help='filepath for the output directory')
    parser.add_argument('--select',
                        default=None,
                        type=str,
                        required=False,
                        help='filepath of the nexson to be produced. If this is passed in, '
                             'then only that study will be exported')
    parser.add_argument('--script-managed-trees',
                        default=None,
                        type=str,
                        required=False,
                        help='path to the script-managed-trees repo')
    parser.add_argument('-v', '--verbose',
                        default=False,
                        action='store_true',
                        help='Verbose mode')
    args = parser.parse_args(sys.argv[1:])
    selected_fn = args.select
    selected_study, selected_tree = None, None
    if selected_fn:
        if selected_fn.endswith('.json'):
            b = selected_fn[:-5]
            b = b.split('_')
            if len(b) > 1:
                selected_study, selected_tree = '_'.join(b[:-1]), b[-1]
                if os.path.split(selected_study)[0]:
                    error('a directory cannot be in the select argument. Use the --output-dir flag.')
                    selected_study = None
        if selected_study is None:
            error('expecting the --select argument to be in the form of "studyID_treeID.json"\n')
    if args.verbose:
        _VERBOSE = True
    out_dir = '.' if args.output_dir is None else args.output_dir
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    # Look up script-managed trees.
    sm_tree_paths = list()
    if args.script_managed_trees:
        trees_index_filename = os.path.join(args.script_managed_trees,'index_of_newicks.txt')
        with open(trees_index_filename) as trees_index_file:
            for line in trees_index_file:
                (order,tree_path) = line.rstrip().split('\t')
                tree_path = os.path.join(args.script_managed_trees,tree_path)
                sm_tree_paths.append(tree_path)

    for sm_tree_path in sm_tree_paths:
        with open(sm_tree_path) as sm_tree_file:
            nexson = newick_to_nexson(sm_tree_file, ott_id_from_label, "tree1")

    # Create a Phylesystem wrapper
    if args.phylesystem_par is not None:
        if not os.path.isdir(args.phylesystem_par):
            sys.exit('Phylesystem parent "{}" is not a directory.\n'.format(args.phylesystem_par))
        ps = Phylesystem(repos_par=args.phylesystem_par)
    else:
        try:
            ps = Phylesystem()
        except:
            sys.stderr.write('Error: You need to use the --phylesystem-par argument, or '
                             'a have a peyotl configuration that includes local phylesystem information.')
            raise
    # Get the list of included trees
    if not os.path.isfile(args.collection):
        error('Input collection "{}" does not exist.\n'.format(args.collection))
        sys.exit(1)
    try:
        included = collection_to_included_trees(args.collection)
    except:
        sys.stderr.write('Error: JSON parse error when reading collection "{}".\n'.format(args.collection))
        raise

    # Remove included trees for studies that have been removed from phylesystem
    included_and_exists = []
#    with ga.lock():
#        ga.checkout_master()
    for inc in included:
        study_id = inc['studyID']
        ga = ps.create_git_action(study_id)
        fp = ga.path_for_doc(study_id)
        if not os.path.isfile(fp):
            debug(fp + ' does not exist: removing from collection on the assumption that the study has been deleted.')
        else:
            included_and_exists.append(inc)
    included = included_and_exists

    # do other things
    included_by_sha = defaultdict(list)
    for inc in included:
        included_by_sha[inc['SHA']].append(inc)
    # map id of input included tree to concrete form
    generic2concrete = {}
    use_latest_trees = included_by_sha['']
    num_moved, num_deleted = 0, 0
    selected_study_found = False
    if selected_study is not None:
        use_latest_trees = [i for i in use_latest_trees if
                            (i['studyID'] == selected_study and i['treeID'] == selected_tree)]
    if use_latest_trees:
        selected_study_found = True
        sha_to_inc = defaultdict(list)
        for ult in use_latest_trees:
            study_id = ult['studyID']
            ga = ps.create_git_action(study_id)
            sha = ga.get_master_sha()
            sha_to_inc[sha].append(ult)
        for sha, from_this_sha_inc in sha_to_inc.items():
            inc = from_this_sha_inc.pop(0)
            study_id = inc['studyID']
            ga = ps.create_git_action(study_id)
            with ga.lock():
                ga.checkout_master()
                if copy_phylesystem_file_if_differing(ga,
                                                      sha,
                                                      inc,
                                                      out_dir,
                                                      generic2concrete):
                    num_moved += 1
                else:
                    num_deleted += 1
            for inc in from_this_sha_inc:
                ga = ps.create_git_action(study_id)
                with ga.lock():
                    ga.checkout_master()
                    if copy_phylesystem_file_if_differing(ga,
                                                          sha, inc,
                                                          out_dir,
                                                          generic2concrete):
                        num_moved += 1
                    else:
                        num_deleted += 1

    for sha, from_this_sha_inc in included_by_sha.items():
        if sha == '':
            continue
        for inc in from_this_sha_inc:
            study_id = inc['studyID']
            if selected_study is not None:
                if selected_study == study_id and inc['treeID'] == selected_tree:
                    selected_study_found = True
                else:
                    continue
            ga = ps.create_git_action(study_id)
            with ga.lock():
                ga.checkout(sha)
                if copy_phylesystem_file_if_differing(ga,
                                                      sha,
                                                      inc,
                                                      out_dir,
                                                      generic2concrete):
                    num_moved += 1
                else:
                    num_deleted += 1
                ga.checkout_master()
    debug('{} total trees'.format(len(included) - num_deleted))
    debug('{} JSON files copied'.format(num_moved))
    debug('{} trees in collections, but with missing studies'.format(num_deleted))
    if selected_study is not None:
        if selected_study_found:
            sys.exit(0)
        error('The selected tree {}_{}.json was not found in the collection\n.'.format(selected_study, selected_tree))
        sys.exit(1)
    # now we write a "concrete" version of this snapshot
    coll_name = os.path.split(args.collection)[-1]
    concrete_collection = get_empty_collection()
    concrete_collection['description'] = 'Concrete form of collection "{}"'.format(coll_name)
    cd_list = concrete_collection['decisions']
    for inc in included:
        try:
            concrete = generic2concrete[id(inc)]
            cd_list.append(concrete)
        except KeyError:
            pass
    concrete_fn = os.path.join(out_dir, 'concrete_' + coll_name)
    write_as_json(concrete_collection, concrete_fn)
