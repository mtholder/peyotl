from peyotl.utility import expand_abspath, get_logger, get_config_setting
import os


def get_phylesystem_parent_list():
    try:
        phylesystem_parent = expand_abspath(get_config_setting('phylesystem', 'parent'))
    except:
        raise ValueError('No [phylesystem] "parent" specified in config or environmental variables')
    # TEMP hardcoded assumption that : does not occur in a path name
    # if present is a separator between parent dirs...
    return phylesystem_parent.split(':')


def get_phylesystem_repo_parent():
    par_list = get_phylesystem_parent_list()
    if len(par_list) != 1:
        if par_list:
            msg = "Multiple repo_parent paths for phylesystem is no longer supported. Found: {}".format(par_list)
            raise NotImplementedError(msg)
    return par_list[0]

# TODO: at some point, we should add another config var. or rename [phylesystem]parent to be more generic
def get_doc_store_repo_parent():
    return get_phylesystem_repo_parent()

_LOG = get_logger(__name__)


def dir_to_repos_dict(dir):
    """Returns a map of subdir name to full path for all subdirectories
    of `dir` that contain a `.git` subdirectory"""
    repos = {}
    absdir = expand_abspath(dir)
    if not os.path.isdir(absdir):
        raise ValueError('Docstore parent "{p}" is not a directory'.format(p=absdir))
    for name in os.listdir(absdir):
        # TODO: Add an option to filter just phylesystem repos (or any specified type?) here!
        #  - add optional list arg `allowed_repo_names`?
        #  - let the FailedShardCreationError work harmlessly?
        #  - treat this function as truly for phylesystem only?
        # noinspection PyTypeChecker
        subdir = os.path.join(absdir, name)
        if os.path.isdir(os.path.join(subdir, '.git')):
            repos[name] = subdir
    return repos


def get_repos(par_list=None):
    """Returns a dictionary of name -> filepath
    `name` is the repo name based on the dir name (not the get repo). It is not
        terribly useful, but it is nice to have so that any mirrored repo directory can
        use the same naming convention.
    `filepath` will be the full path to the repo directory (it will end in `name`)
    """
    _repos = {}  # key is repo name, value repo location
    if par_list is None:
        par_list = [get_phylesystem_repo_parent()]
    elif not isinstance(par_list, list):
        par_list = [par_list]
    for p in par_list:
        r = dir_to_repos_dict(p)
        _repos.update(r)
    if len(_repos) == 0:
        raise ValueError('No git repos in {parent}'.format(parent=str(par_list)))
    return _repos
