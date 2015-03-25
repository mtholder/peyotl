#!/usr/bin/env python
from peyotl.supertree import OtcSupertreeSubproblem

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        sys.exit('''Expecting 3 arguments:
    an input treefile
    a simplification output directory
    a solution directory
''')
    subproblem = OtcSupertreeSubproblem(filepath=sys.argv[1])
    simplification_dir, solution_dir = sys.argv[2:]
    r = subproblem.solve_or_simplify(solution_dir, simplification_dir)
