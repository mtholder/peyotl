#!/usr/bin/env python
from peyotl.phylo.tree import _TreeWithNodeIDs, parse_newick
from peyotl.phylo.compat import PhyloStatement

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        sys.exit('Expecting a treefile as an argument')
    tree = parse_newick(filepath=sys.argv[1], _class=_TreeWithNodeIDs)
