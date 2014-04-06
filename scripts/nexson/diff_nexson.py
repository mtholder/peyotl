#!/usr/bin/env python
from peyotl.nexson_diff import NexsonDiff
import sys
VERBOSE = False

def debug(m):
    if VERBOSE:
        sys.stderr.write('diff_nexson.py: {}\n'.format(m))
def error(m):
    sys.stderr.write('diff_nexson.py: {}\n'.format(m))

def _main():
    global VERBOSE
    import codecs, json
    import argparse
    _HELP_MESSAGE = '''NexSON diff tool'''
    _EPILOG = '''UTF-8 encoding is used (for input and output).

Environmental variables used:
    NEXSON_INDENTATION_SETTING indentation in NexSON (default 0)
'''
    parser = argparse.ArgumentParser(description=_HELP_MESSAGE,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=_EPILOG)
    parser.add_argument("input1", help="filepath to the ancestor file")
    parser.add_argument("input2", help="filepath to the descendant file")
    parser.add_argument("-o", "--output", 
                        metavar="FILE",
                        required=False,
                        help="output filepath. Standard output is used if omitted.")
    parser.add_argument("-v", "--verbose", 
                        action="store_true",
                        default=False,
                        help="Verbose mode.")
    args = parser.parse_args()
    inpfn1 = args.input1
    inpfn2 = args.input2
    outfn = args.output
    VERBOSE = args.verbose
    try:
        inp1 = codecs.open(inpfn1, mode='rU', encoding='utf-8')
    except:
        error('Could not open file "{fn}"\n'.format(fn=inpfn1))
        return False
    try:
        inp2 = codecs.open(inpfn2, mode='rU', encoding='utf-8')
    except:
        error('Could not open file "{fn}"\n'.format(fn=inpfn2))
        return False

    nd = NexsonDiff(inp1, inp2)
    if nd.has_differences():
        od = nd.as_ot_diff_dict()
        if outfn is not None:
            try:
                out = codecs.open(outfn, mode='w', encoding='utf-8')
            except:
                sys.exit('nexson_diff: Could not open output filepath "{fn}"\n'.format(fn=outfn))
        else:
            out = codecs.getwriter('utf-8')(sys.stdout)
        debug(repr(od))
        json.dump(od, out, indent=2, sort_keys=True)
        out.write('\n')
    else:
        debug('No differences')
    return True

if __name__ == '__main__':
    if not _main():
        sys.exit(1)
