from sh import git
import sh 
import re
import os
import locket
import codecs
from peyotl import get_logger
import shutil
from peyotl.nexson_syntax import write_as_json
import tempfile #@TEMPORARY for deprecated write_study
_LOG = get_logger(__name__)
class MergeException(Exception):
    pass


def get_HEAD_SHA1(git_dir):
    '''Not locked!
    '''
    head_file = os.path.join(git_dir, 'HEAD')
    with open(head_file, 'rU') as hf:
        head_contents = hf.read().strip()
    assert(head_contents.startswith('ref: '))
    ref_filename = head_contents[5:] #strip off "ref: "
    real_ref = os.path.join(git_dir, ref_filename)
    with open(real_ref, 'rU') as rf:
        return rf.read().strip()


class GitWorkflowError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def get_user_author(auth_info):
    '''Return commit author info from a dict. Returns username and author string.

    auth_info should have 3 strings:
        `login` (a github log in)
        `name`
        `email`
    username will be in the `login` value. It is used for WIP branch naming.
    the author string will be the name and email joined. This is used in commit messages.
    '''
    return auth_info['login'], get_author(auth_info)

def get_author(auth_info):
    return "{} <{}>".format(auth_info['name'], auth_info['email'])

def get_filepath_for_namespaced_id(repo_dir, study_id):
    if len(study_id) < 4:
        while len(study_id) < 2:
            study_id = '0' + study_id
        study_id = 'pg_' + study_id
    elif study_id[2] != '_':
        study_id = 'pg_' + study_id
    frag = study_id[-2:]
    while len(frag) < 2:
        frag = '0' + frag
    dest_topdir = study_id[:3] + frag
    dest_subdir = study_id
    dest_file = dest_subdir + '.json'
    return os.path.join(repo_dir, 'study', dest_topdir, dest_subdir, dest_file)

def get_filepath_for_simple_id(repo_dir, study_id):
    return '{r}/study/{s}/{s}.json'.format(r=repo_dir, s=study_id)

class RepoLock():
    def __init__(self, lock):
        self._lock = lock
    def __enter__(self):
        self._lock.acquire()
    def __exit__(self, _type, value, traceb):
        self._lock.release()

class GitAction(object):
    @staticmethod
    def clone_repo(par_dir, repo_local_name, remote):
        if not os.path.isdir(par_dir):
            raise ValueError(repr(par_dir) + ' is not a directory')
        if not (isinstance(remote, str) or isinstance(remote, unicode)):
            raise ValueError(repr(remote) + ' is not a remote string')
        dest = os.path.join(par_dir, repo_local_name)
        if os.path.exists(dest):
            raise RuntimeError('Filepath "{}" is in the way'.format(dest))
        git('clone', remote, repo_local_name, _cwd=par_dir)
    @staticmethod
    def add_remote(repo_dir, remote_name, remote_url):
        git_dir_arg = "--git-dir={}/.git".format(repo_dir)
        git(git_dir_arg, 'remote', 'add', remote_name, remote_url)

    def __init__(self,
                 repo,
                 remote=None,
                 git_ssh=None,
                 pkey=None,
                 cache=None, 
                 path_for_study_fn=None):
        """Create a GitAction object to interact with a Git repository

        Example:
        gd   = GitAction(repo="/home/user/git/foo")

        Note that this requires write access to the
        git repository directory, so it can create a
        lockfile in the .git directory.

        """
        self.repo = repo
        self.git_dir = os.path.join(repo, '.git')
        self._lock_file = os.path.join(self.git_dir, "API_WRITE_LOCK")
        self._lock_timeout = 30
        self._lock = locket.lock_file(self._lock_file, timeout=self._lock_timeout)
        self.repo_remote = remote
        self.git_ssh = git_ssh
        self.pkey = pkey
        
        if os.path.isdir("{}/.git".format(self.repo)):
            self.git_dir_arg = "--git-dir={}/.git".format(self.repo)
            self.gitwd = "--work-tree={}".format(self.repo)
        else: #EJM needs a test?
            raise ValueError('Repo "{repo}" is not a git repo'.format(repo=self.repo))
        if path_for_study_fn is None:
            self.path_for_study_fn = get_filepath_for_simple_id
        else:
            self.path_for_study_fn = path_for_study_fn
    def lock(self):
        ''' for syntax:
        with git_action.lock():
            git_action.checkout('master')
        '''
        return RepoLock(self._lock)

    def path_for_study(self, study_id):
        '''Returns study_dir and study_filepath for study_id.
        '''
        return self.path_for_study_fn(self.repo, study_id)

    def relative_path_for_study(self):
        p = self.path_for_study('', study_id)
        assert(p.startswith('/study'))
        return p[1:]

    def env(self): #@TEMP could be ref to a const singleton.
        d = dict(os.environ)
        if self.git_ssh:
            d['GIT_SSH'] = self.git_ssh
        if self.pkey:
            d['PKEY'] = self.pkey
        return d

    def acquire_lock(self):
        "Acquire a lock on the git repository"
        _LOG.debug('Acquiring lock')
        self._lock.acquire()

    def release_lock(self):
        "Release a lock on the git repository"
        _LOG.debug('Releasing lock')
        try:
            self._lock.release()
        except:
            _LOG.debug('Exception releasing lock suppressed.')

    def current_branch(self):
        "Return the current branch name"
        branch_name = git(self.git_dir_arg, self.gitwd, "symbolic-ref", "HEAD")
        return branch_name.replace('refs/heads/', '').strip()

    def checkout_master(self):
        git(self.git_dir_arg, self.gitwd, "checkout", "master")

    def get_mrca_sha(self, branch_or_sha1, branch_or_sha2):
        '''Takes two commit references (branch names or commit SHAs)
        and returns the sha of their most recent common ancestor
        '''
        x = git(self.git_dir_arg, 'merge-base', branch_or_sha1, branch_or_sha2)
        return x.strip()

    def write_study_contents_for_commit(self, commit_sha, study_id, out_file):
        v_arg = '{c}:{s}'.format(c=commit_sha, s=self.relative_path_for_study())
        git(self.git_dir_arg, 'cat-file', '-p', v_arg, _out=out_file)

    def _head_sha(self):
        return git(self.git_dir_arg, self.gitwd, "rev-parse", "HEAD").strip()

    def get_master_sha(self):
        x = git(self.git_dir_arg, self.gitwd, "show-ref", "master", "--heads", "--hash")
        return x.strip()


        # next we must look at local branch names for new studies
        # without --no-color we get terminal color codes in the branch output
        #branches = git(self.git_dir_arg, self.gitwd, "branch", "--no-color")
        #branches = [ b.strip() for b in branches ]
        #for b in branches:
        #    mo = re.match(".+_o(\d+)", b)
        #    if mo:
        #        dirs.append(int(mo.group(1)))
        #dirs.sort()
        #return dirs[-1]

    def return_study(self, study_id, branch='master', commit_sha=None, return_WIP_map=False): 
        """Return the 
            blob[0] contents of the given study_id, 
            blob[1] the SHA1 of the HEAD of branch (or `commit_sha`)
            blob[2] dictionary of WIPs for this study.
        If the study_id does not exist, it returns the empty string.
        If `commit_sha` is provided, that will be checked out and returned.
            otherwise the branch will be checked out.
        """
        #_LOG.debug('return_study({s}, {b}, {c}...)'.format(s=study_id, b=branch, c=commit_sha))
        if commit_sha is None:
            self.checkout(branch)
            head_sha = get_HEAD_SHA1(self.git_dir)
        else:
            self.checkout(commit_sha)
            head_sha = commit_sha
        study_filepath = self.path_for_study(study_id)
        try:
            f = codecs.open(study_filepath, mode='rU', encoding='utf-8')
            content = f.read()
        except:
            content = ''
        if return_WIP_map:
            d = self.find_WIP_branches(study_id)
            return content, head_sha, d
        return content, head_sha

    def branch_exists(self, branch):
        """Returns true or false depending on if a branch exists"""
        try:
            git(self.git_dir_arg, self.gitwd, "rev-parse", branch)
        except sh.ErrorReturnCode:
            return False
        return True

    def find_WIP_branches(self, study_id):
        pat = re.compile(r'.*_study_{i}_[0-9]+'.format(i=study_id))
        head_shas = git(self.git_dir_arg, self.gitwd, "show-ref", "--heads")
        ret = {}
        #_LOG.debug('find_WIP_branches head_shas = "{}"'.format(head_shas.split('\n')))
        for lin in head_shas.split('\n'):
            try:
                local_branch_split = lin.split(' refs/heads/')
                if len(local_branch_split) == 2:
                    sha, branch = local_branch_split
                    if pat.match(branch) or branch == 'master':
                        ret[branch] = sha
            except:
                raise
        return ret

    def _find_head_sha(self, frag, parent_sha):
        head_shas = git(self.git_dir_arg, self.gitwd, "show-ref", "--heads")
        for lin in head_shas.split('\n'):
            #_LOG.debug("lin = '{l}'".format(l=lin))
            if lin.startswith(parent_sha):
                local_branch_split = lin.split(' refs/heads/')
                #_LOG.debug("local_branch_split = '{l}'".format(l=local_branch_split))
                if len(local_branch_split) == 2:
                    branch = local_branch_split[1].rstrip()
                    if branch.startswith(frag):
                        return branch
        return None
    def checkout(self, branch):
        git(self.git_dir_arg, self.gitwd, "checkout", branch)

    def create_or_checkout_branch(self, gh_user, study_id, parent_sha, force_branch_name=False):
        if force_branch_name:
            #@TEMP deprecated
            branch = "{ghu}_study_{rid}".format(ghu=gh_user, rid=study_id)
            if not self.branch_exists(branch):
                try:
                    git(self.git_dir_arg, self.gitwd, "branch", branch, parent_sha)
                    _LOG.debug('Created branch "{b}" with parent "{a}"'.format(b=branch, a=parent_sha))
                except:
                    raise ValueError('parent sha not in git repo')
            self.checkout(branch)
            return branch

        frag = "{ghu}_study_{rid}_".format(ghu=gh_user, rid=study_id)
        branch = self._find_head_sha(frag, parent_sha)
        _LOG.debug('Found branch "{b}" for sha "{s}"'.format(b=branch, s=parent_sha))
        if not branch:
            branch = frag + '0'
            i = 1
            while self.branch_exists(branch):
                branch = frag + str(i)
                i += 1
            _LOG.debug('lowest non existing branch =' + branch)
            try:
                git(self.git_dir_arg, self.gitwd, "branch", branch, parent_sha)
                _LOG.debug('Created branch "{b}" with parent "{a}"'.format(b=branch, a=parent_sha))
            except:
                raise ValueError('parent sha not in git repo')
        self.checkout(branch)
        _LOG.debug('Checked out branch "{b}"'.format(b=branch))
        return branch

    def fetch(self, remote='origin'):
        '''fetch from a remote'''
        git(self.git_dir_arg, "fetch", remote)
    
    def push(self, branch, remote):
        git(self.git_dir_arg, 'push', remote, branch, _env=self.env())

    #@TEMP TODO. Args should be gh_user, study_id, parent_sha, author but 
    #   currently using the # of args as a hack to detect whether the
    #   old or newer version of the function is required. #backward-compat. @KILL with merge of local-dep
    def remove_study(self, first_arg, sec_arg, third_arg, fourth_arg=None):
        """Remove a study
        Given a study_id, branch and optionally an
        author, remove a study on the given branch
        and attribute the commit to author.
        Returns the SHA of the commit on branch.
        """
        if fourth_arg is None:
            study_id, branch_name, author = first_arg, sec_arg, third_arg
            #@TODO. DANGER super-ugly hack to get gh_user 
            #   only doing this function is going away very soon. @KILL with merge of local-dep
            gh_user = branch_name.split('_study_')[0]
            parent_sha = self.get_master_sha()
        else:
            gh_user, study_id, parent_sha, author = first_arg, sec_arg, third_arg, fourth_arg
        study_path = self.path_for_study(study_id)
        study_dir = os.path.split(study_path)[0]

        branch = self.create_or_checkout_branch(gh_user, study_id, parent_sha)
        if not os.path.isdir(study_dir):
            # branch already exists locally with study removed
            # so just return the commit SHA
            return self._head_sha(), branch
        git(self.git_dir_arg, self.gitwd, "rm", "-rf", study_dir)
        git(self.git_dir_arg, self.gitwd, "commit", author=author, message="Delete Study #%s via OpenTree API" % study_id)
        return self._head_sha(), branch
        

    def reset_hard(self):
        try:
            git(self.git_dir_arg, self.gitwd, 'reset', '--hard')
        except:
            _LOG.exception('"git reset --hard" failed.')

    def get_blob_sha_for_file(self, filepath, branch='HEAD'):
        try:
            r = git(self.git_dir_arg, self.gitwd, 'ls-tree', branch, filepath)
            #_LOG.debug('ls-tree said "{}"'.format(r))
            line = r.strip()
            ls = line.split()
            #_LOG.debug('ls is "{}"'.format(str(ls)))
            assert(len(ls) == 4)
            assert(ls[1] == 'blob')
            return ls[2]
        except:
            _LOG.exception('git ls-tree failed')
            raise

    def write_study_from_tmpfile(self, study_id, tmpfi, parent_sha, auth_info):
        """Given a study_id, temporary filename of content, branch and auth_info
        """
        gh_user, author = get_user_author(auth_info)
        study_filepath = self.path_for_study(study_id)
        study_dir = os.path.split(study_filepath)[0]
        if parent_sha is None:
            self.checkout_master()
            parent_sha = self.get_master_sha()
        branch = self.create_or_checkout_branch(gh_user, study_id, parent_sha)
        
        # create a study directory if this is a new study EJM- what if it isn't?
        if not os.path.isdir(study_dir):
            os.makedirs(study_dir)
        
        if os.path.exists(study_filepath):
            prev_file_sha = self.get_blob_sha_for_file(study_filepath)
        else:
            prev_file_sha = None
        shutil.copy(tmpfi.name, study_filepath)
        commit_sha = self._add_and_commit_from_working_dir(study_id, author, study_filepath)
        _LOG.debug('Committed study "{i}" to branch "{b}" commit SHA: "{s}"'.format(i=study_id, b=branch, s=commit_sha))
        return {'commit_sha': commit_sha,
                'prev_file_sha': prev_file_sha,
                'branch': branch,
               }

    def _add_and_commit_from_working_dir(self, study_id=None, author=None, study_filepath=None):
        if study_id and (not study_filepath):
            study_filepath = self.path_for_study(study_id)
        if study_filepath:
            git(self.git_dir_arg, self.gitwd, "add", study_filepath)
        try:
            m = "Update Study #%s via OpenTree API" % study_id
            if study_filepath:
                if author:
                    git(self.git_dir_arg, self.gitwd, "commit", author=author, message=m)
                else:
                    git(self.git_dir_arg, self.gitwd, "commit", message=m)
            else:
                if author:
                    git(self.git_dir_arg, self.gitwd, "commit", "-a", author=author, message=m)
                else:
                    git(self.git_dir_arg, self.gitwd, "commit", "-a", message=m)
        except Exception, e:
            # We can ignore this if no changes are new,
            # otherwise raise a 400
            if "nothing to commit" in e.message:#@EJM is this dangerous?
                 pass
            else:
                _LOG.exception('"git commit" failed')
                self.reset_hard()
                raise
        return self._head_sha()

    def merge(self, branch, destination="master", auth_info=None):
        """
        Merge the the given WIP branch to master (or destination, if specified)

        If the merge fails, the merge will be aborted
        and then a MergeException will be thrown. The
        message of the MergeException will be the
        "git status" output, so details about merge
        conflicts can be determined.
        """
        current_branch = self.current_branch()
        if current_branch != destination:
            _LOG.debug('checking out ' + destination)
            git(self.git_dir_arg, self.gitwd, "checkout", destination)
        author = None
        if auth_info:
            author = get_author(auth_info)
        try:
            git(self.git_dir_arg, self.gitwd, "merge", "--no-commit", branch)
            self._add_and_commit_from_working_dir(author=author)
        except sh.ErrorReturnCode:
            _LOG.exception('merge failed')
            # attempt to reset things so other operations can continue
            git(self.git_dir_arg, self.gitwd, "merge", "--abort")
            # raise an MergeException so that caller will know that the merge failed
            raise MergeException()
        return self._head_sha()

    def conflicted_merge(self, branch, destination, study_id, auth_info):
        '''Attempts to use NexsonDiffs to resolve a conflict 
        between different versions of the file for `study_id`
        between `branch` and `destination`
        Commits to `destination` if successful.

        #TODO this holds the lock too long...
        '''
        current_branch = self.current_branch()
        if current_branch != destination:
            _LOG.debug('checking out ' + destination)
            git(self.git_dir_arg, self.gitwd, "checkout", destination)
        try:
            git(self.git_dir_arg, self.gitwd, "merge", branch)
            return {'diff_that_patched': None,
                    'post_merge_diff': None,
                    'files_to_del': [],
                    'sha': self._head_sha(),
            }
        except:
            pass
        mrca_sha = self.get_master_sha(branch, destination)
        study_filepath = self.path_for_study(study_id)
        author = get_author(auth_info)
        mfn, mfo, sfo, sfn, dfo, dfn = None, None, None, None, None, None
        try:
            mfo, mfn = _create_tempfile_utf8(suffix='{i}-mrca.json'.format(i=study_id))
            self.write_study_contents_for_commit(mrca_sha, study_id, mfo)
            mfo.close()
            sfo, sfn = _create_tempfile_utf8(suffix='{i}-src.json'.format(i=study_id))
            self.write_study_contents_for_commit(branch, study_id, sfo)
            sfo.close()
            dfo, dfn = _create_tempfile_utf8(suffix='{i}-dest.json'.format(i=study_id))
            self.write_study_contents_for_commit(destination, study_id, dfo)
            dfo.close()
            shutil.copy(sfn, study_filepath) # start with the content in the source
            edits_on_dest = NexsonDiff(mfn, dfn) # construct differences made by this curator (diff between mrca and dest branch)
            edits_on_dest.patch_modified_file(study_filepath) # apply the curator's edits to the base
            self._add_and_commit_from_working_dir(study_id, author, study_filepath) # commit the result
            diffs_from_dest_par = NexsonDiff(dfn, study_filepath) # figure out how the dest differs from what was just committed.
        except:
            try:
                if mfo is not None:
                    mfo.close()
            except:
                pass
            try:
                if mfn is not None:
                    os.remove(mfn)
            except:
                pass
            try:
                if sfo is not None:
                    sfo.close()
            except:
                pass
            try:
                if sfn is not None:
                    os.remove(sfn)
            except:
                pass
            try:
                if dfo is not None:
                    dfo.close()
            except:
                pass
            try:
                if dfn is not None:
                    os.remove(dfn)
            except:
                pass
            raise
        return {'diff_that_patched': edits_on_dest,
                'post_merge_diff': diffs_from_dest_par,
                'files_to_del': [mfn, sfn, dfn],
                'sha': self._head_sha(),
        }
        
    def delete_branch(self, branch):
        git(self.git_dir_arg, self.gitwd, 'branch', '-d', branch)

def _create_tempfile_utf8(suffix):
    '''Creates (using mkstemp) a temporary file with the
    given suffix. 
    returns an open (for writing) file object with the utf-8 encoding codec.
    and the filepath
    '''
    fd, fn = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    return codecs.open(fn, 'w', encoding='utf-8'), fn