#!/usr/bin/env python
from peyotl.nexson_diff import NexsonDiff
import sys
import os
VERBOSE = False

def debug(m):
    if VERBOSE:
        sys.stderr.write('patch_nexson.py: {}\n'.format(m))
def error(m):
    sys.stderr.write('patch_nexson.py: {}\n'.format(m))

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
    parser.add_argument("base", help="filepath to the file that the patch will be applied to")
    parser.add_argument("altered", help="filepath to an altered file that has the desired changes")
    parser.add_argument("mrca", help="filepath to the most recent common ancestor of 'base' and 'altered'")
    parser.add_argument("-o", "--output", 
                        metavar="FILE",
                        required=True,
                        help="output filepath. Standard output is used if omitted.")
    parser.add_argument("-v", "--verbose", 
                        action="store_true",
                        default=False,
                        help="Verbose mode.")
    args = parser.parse_args()
    basefile, edited, mrca = args.base, args.altered, args.mrca
    for fp in [basefile, edited, mrca]:
        if not os.path.isfile(fp):
            error('"{}" does not exist'.format(inpfn1))
            return False
    VERBOSE = args.verbose
    nd = NexsonDiff(anc=mrca, des=edited)
    nd.patch_modified_file(basefile, output_filepath=args.output)
    return True

if __name__ == '__main__':
    if not _main():
        sys.exit(1)
