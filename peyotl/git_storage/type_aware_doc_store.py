"""Base class for "type-aware" (sharded) document storage. This goes beyond simple subclasses
like PhylesystemProxy by introducing more business rules and differences between document types
in the store (eg, Nexson studies in Phylesystem, tree collections in TreeCollectionStore)."""
import os
from threading import Lock
from peyotl.utility.imports import StringIO
import anyjson
from peyotl.git_storage import ShardedDocStore
from peyotl.git_storage.helper import get_repos
from peyotl.git_storage.git_shard import FailedShardCreationError
from peyotl.utility import get_logger

_LOG = get_logger(__name__)


def parse_mirror_info(mirror_info):
    push_mirror_repos_par = None
    push_mirror_remote_map = {}
    if mirror_info:
        push_mirror_info = mirror_info.get('push', {})
        if push_mirror_info:
            push_mirror_repos_par = push_mirror_info['parent_dir']
            push_mirror_remote_map = push_mirror_info.get('remote_map', {})
            if push_mirror_repos_par:
                if not os.path.exists(push_mirror_repos_par):
                    os.makedirs(push_mirror_repos_par)
                if not os.path.isdir(push_mirror_repos_par):
                    e_fmt = 'Specified push_mirror_repos_par, "{}", is not a directory'
                    e = e_fmt.format(push_mirror_repos_par)
                    raise ValueError(e)
    return push_mirror_repos_par, push_mirror_remote_map


class TypeAwareDocStore(ShardedDocStore):
    def __init__(self,
                 path_mapper=None,
                 repos_dict=None,
                 repos_par=None,
                 git_shard_class=None,  # requires a *type-specific* GitShard subclass
                 mirror_info=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 shard_mirror_pair_list=None):
        """
        Repos can be found by passing in a `repos_par` (a directory that is the parent of the repos)
            or by trusting the `repos_dict` mapping of name to repo filepath.
        `with_caching` should be True for non-debugging uses.
        If you want to use a mirrors of the repo for pushes or pulls, send in a `mirror_info` dict:
            mirror_info['push'] and mirror_info['pull'] should be dicts with the following keys:
            'parent_dir' - the parent directory of the mirrored repos
            'remote_map' - a dictionary of remote name to prefix (the repo name + '.git' will be
                appended to create the URL for pushing).
        """
        ShardedDocStore.__init__(self, path_mapper=path_mapper)
        _LOG.debug('TypeAwareDocStore(repo_par={}, repos_dict={})'.format(repos_par, repos_dict))
        self._growing_shard = None
        # TODO should infer doc prefix and hard-code assumed_doc_version to None
        shards = []
        if shard_mirror_pair_list is not None:
            self._filepath_args = 'shard_mirror_pair_list = {}'.format(repr(shard_mirror_pair_list))
            push_mirror_repos_par, push_mirror_remote_map = None, {}
            for repo_filepath, push_mirror_repo_path in shard_mirror_pair_list:
                try:
                    repo_name = os.path.split(repo_filepath)[1]
                    # assumes uniform __init__ arguments for all GitShard subclasses
                    shard = git_shard_class(name=repo_name,
                                            path=repo_filepath,
                                            push_mirror_repo_path=push_mirror_repo_path)
                    shards.append(shard)
                except FailedShardCreationError as x:
                    f = 'SKIPPING repo "{d}" (not a {c}). Details:\n  {e}'
                    f = f.format(d=repo_filepath, c=git_shard_class.__name__, e=str(x))
                    _LOG.warn(f)
        else:
            if repos_dict is not None:
                self._filepath_args = 'repos_dict = {}'.format(repr(repos_dict))
            elif repos_par is not None:
                self._filepath_args = 'repos_par = {}'.format(repr(repos_par))
            else:
                self._filepath_args = '<No arg> default phylesystem_parent from env/config cascade'
            push_mirror_repos_par, push_mirror_remote_map = parse_mirror_info(mirror_info=mirror_info)

            if repos_dict is None:
                repos_dict = get_repos(repos_par)
            repo_name_list = list(repos_dict.keys())
            repo_name_list.sort()
            for repo_name in repo_name_list:
                repo_filepath = repos_dict[repo_name]
                push_mirror_repo_path = None
                if push_mirror_repos_par:
                    expected_push_mirror_repo_path = os.path.join(push_mirror_repos_par, repo_name)
                    if os.path.isdir(expected_push_mirror_repo_path):
                        push_mirror_repo_path = expected_push_mirror_repo_path
                try:
                    # assumes uniform __init__ arguments for all GitShard subclasses
                    shard = git_shard_class(name=repo_name,
                                            path=repo_filepath,
                                            push_mirror_repo_path=push_mirror_repo_path,
                                            infrastructure_commit_author=infrastructure_commit_author)
                except FailedShardCreationError as x:
                    f = 'SKIPPING repo "{d}" (not a {c}). Details:\n  {e}'
                    f = f.format(d=repo_filepath, c=git_shard_class.__name__, e=str(x))
                    _LOG.warn(f)
                    continue
                # if the mirror does not exist, clone it...
                if push_mirror_repos_par and (push_mirror_repo_path is None):
                    from peyotl.git_storage import GitActionBase
                    GitActionBase.clone_repo(push_mirror_repos_par,
                                             repo_name,
                                             repo_filepath)
                    if not os.path.isdir(expected_push_mirror_repo_path):
                        e_msg = 'git clone in mirror bootstrapping did not produce a directory at {}'
                        e = e_msg.format(expected_push_mirror_repo_path)
                        raise ValueError(e)
                    for remote_name, remote_url_prefix in push_mirror_remote_map.items():
                        if remote_name in ['origin', 'originssh']:
                            f = '"{}" is a protected remote name in the mirrored repo setup'
                            m = f.format(remote_name)
                            raise ValueError(m)
                        remote_url = remote_url_prefix + '/' + repo_name + '.git'
                        GitActionBase.add_remote(expected_push_mirror_repo_path, remote_name, remote_url)
                    shard.push_mirror_repo_path = expected_push_mirror_repo_path
                    for remote_name in push_mirror_remote_map.keys():
                        mga = shard.create_git_action_for_mirror()  # pylint: disable=W0212
                        mga.fetch(remote_name)
                shards.append(shard)
        assert len(shards) > 0
        #  New convention: only one shard has a
        #   `new_study_prefix`, so only one shard can generate new IDs. There should only be one shard
        #   with `can_mint_new_docs() set to True
        growing_shards = [i for i in shards if i.can_mint_new_docs()]
        #_LOG.debug('shards = {} growing_shards = {}'.format(shards, growing_shards))
        assert len(growing_shards) == 1
        self._growing_shard = growing_shards[-1]
        self.doc_schema = self._growing_shard.doc_schema
        self._shards = shards
        self._prefix2shard = {}
        for shard in shards:
            for prefix in shard.known_prefixes:
                # we don't currently support multiple shards with the same ID prefix scheme
                assert prefix not in self._prefix2shard
                self._prefix2shard[prefix] = shard
        with self._index_lock:
            self._locked_refresh_doc_ids()

    def create_git_action_for_new_document(self, new_doc_id=None):
        """Checks out master branch of the shard as a side effect"""
        return self._growing_shard.create_git_action_for_new_doc(new_doc_id=new_doc_id)

    def _locked_refresh_doc_ids(self):
        """Assumes that the caller has the _index_lock !
        """
        d = {}
        for s in self._shards:
            for k in s.doc_index.keys():
                if k in d:
                    raise KeyError('doc "{i}" found in multiple repos'.format(i=k))
                d[k] = s
        self._doc2shard_map = d

    def has_doc(self, doc_id):
        with self._index_lock:
            return doc_id in self._doc2shard_map

    def create_git_action(self, doc_id):
        shard = self.get_shard(doc_id)
        if shard is None:
            shard = self._growing_shard
        assert shard is not None
        return shard.create_git_action()

    def get_filepath_for_doc(self, doc_id):
        ga = self.create_git_action(doc_id)
        return ga.path_for_doc(doc_id)

    def return_doc(self,
                   doc_id,
                   branch='master',
                   commit_sha=None,
                   return_WIP_map=False):
        ga = self.create_git_action(doc_id)
        with ga.lock():
            return self._return_doc_already_locked(ga,
                                                   doc_id=doc_id,
                                                   branch=branch,
                                                   commit_sha=commit_sha,
                                                   return_WIP_map=return_WIP_map)

    return_document = return_doc

    def return_document_and_history(self, doc_id, branch='master', commit_sha=None, return_WIP_map=True):
        """Returns a pair the first element is the tuple returned by return_document and the second
        is the response from get_version_history_for_doc_id.

        This is done with one holding of the lock.

        TODO: need to pass in some args to get the history only up to commit_sha"""
        ga = self.create_git_action(doc_id)
        with ga.lock():
            doc = self._return_doc_already_locked(ga,
                                                  doc_id=doc_id,
                                                  branch=branch,
                                                  commit_sha=commit_sha,
                                                  return_WIP_map=return_WIP_map)
            docpath = ga.path_for_doc(doc_id)
            history = ga.get_version_history_for_file(docpath)
        return doc, history

    def _return_doc_already_locked(self, ga, doc_id, branch, commit_sha, return_WIP_map):
        blob = ga.return_document(doc_id,
                                  branch=branch,
                                  commit_sha=commit_sha,
                                  return_WIP_map=return_WIP_map)
        content = blob[0]
        if content is None:
            raise KeyError('Document {} not found'.format(doc_id))
        nexson = anyjson.loads(blob[0])
        if return_WIP_map:
            return nexson, blob[1], blob[2]
        return nexson, blob[1]

    def get_blob_sha_for_doc_id(self, doc_id, head_sha):
        ga = self.create_git_action(doc_id)
        docpath = ga.path_for_doc(doc_id)
        return ga.get_blob_sha_for_file(docpath, head_sha)

    def get_version_history_for_doc_id(self, doc_id):
        ga = self.create_git_action(doc_id)
        docpath = ga.path_for_doc(doc_id)
        return ga.get_version_history_for_file(docpath)

    def push_doc_to_remote(self, remote_name, doc_id=None):
        """This will push the master branch to the remote named `remote_name`
        using the mirroring strategy to cut down on locking of the working repo.

        `doc_id` is used to determine which shard should be pushed.
        if `doc_id` is None, all shards are pushed.
        """
        if doc_id is None:
            ret = True
            # @TODO should spawn a thread of each shard...
            for shard in self._shards:
                if not shard.push_to_remote(remote_name):
                    ret = False
            return ret
        shard = self.get_shard(doc_id)
        return shard.push_to_remote(remote_name)

    def commit_and_try_merge2master(self,
                                    file_content,
                                    doc_id,
                                    auth_info,
                                    parent_sha,
                                    commit_msg='',
                                    merged_sha=None):
        from peyotl.git_storage.git_workflow import generic_commit_and_try_merge2master_wf
        git_action = self.create_git_action(doc_id)
        resp = generic_commit_and_try_merge2master_wf(git_action,
                                                      file_content,
                                                      doc_id,
                                                      auth_info,
                                                      parent_sha,
                                                      commit_msg,
                                                      merged_sha=merged_sha)
        if not resp['merge_needed']:
            self._doc_merged_hook(git_action, doc_id)
        return resp

    def validate_and_convert_doc(self, document, write_arg_dict):
        """Helper function for phyleystem-api. Takes the document to be written and a dict
        with:
            `auth_info`: dict of author info
            'starting_commit_SHA' SHA of parent of commit
            'commit_msg' content of commit message
            'merged_SHA' -> bool,
        'doc_id' may be present if this is being called in an edit rather than creation context

        In the case of a DocStore that supports translations among formats, this method
        should perform the requested conversion

        The method should return a tuple with four items:
            [0] a processed form of the document (`document` or the converted form),
            [1] a list of strings describing blocking errors
            [2] an annotation object for the document (or None if no annotations are used), and
            [3] an adaptor object.
        """
        return self._growing_shard.validate_annotate_convert_doc(document, **write_arg_dict)

    def annotate_and_write(self,
                           document,
                           doc_id,
                           auth_info,
                           adaptor,
                           annotation,
                           parent_sha,
                           commit_msg='',
                           merged_sha=None,
                           add_agent_only=True):
        """
        This is the heart of the api's __finish_write_verb
        It was moved to phylesystem to make it easier to coordinate it
            with the caching decisions. We have been debating whether
            to cache @id and @dateCreated attributes for the annotations
            or cache the whole annotation. Since these decisions are in
            add_validation_annotation (above), it is easier to have
            that decision and the add_or_replace_annotation call in the
            same repo.
        """
        adaptor.add_or_replace_annotation(document,
                                          annotation,
                                          add_agent_only=add_agent_only)
        return self.commit_and_try_merge2master(file_content=document,
                                                doc_id=doc_id,
                                                auth_info=auth_info,
                                                parent_sha=parent_sha,
                                                commit_msg=commit_msg,
                                                merged_sha=merged_sha)

    def delete_doc(self, doc_id, auth_info, parent_sha, **kwargs):
        git_action = self.create_git_action(doc_id)
        from peyotl.git_storage.git_workflow import delete_document
        doctype_display_name = kwargs.get('doctype_display_name', None)
        ret = delete_document(git_action,
                              doc_id,
                              auth_info,
                              parent_sha,
                              doctype_display_name=doctype_display_name,
                              **kwargs)
        if not ret['merge_needed']:
            with self._index_lock:
                try:
                    _shard = self._doc2shard_map[doc_id]
                except KeyError:
                    pass
                else:
                    try:
                        del self._doc2shard_map[doc_id]
                    except KeyError:
                        pass
                    _shard.delete_doc_from_index(doc_id)
        return ret

    def iter_doc_objs(self, **kwargs):
        """Generator that iterates over all detected documents (eg, nexson studies)
        and returns the doc object (deserialized from JSON) for each doc.
        Order is by shard, but arbitrary within shards.
        @TEMP not locked to prevent doc creation/deletion
        """
        for shard in self._shards:
            for doc_id, blob in shard.iter_doc_objs(**kwargs):
                yield doc_id, blob

    def iter_doc_filepaths(self, **kwargs):
        """Generator that iterates over all detected documents.
        and returns the filesystem path to each doc.
        Order is by shard, but arbitrary within shards.
        @TEMP not locked to prevent doc creation/deletion
        """
        for shard in self._shards:
            for doc_id, blob in shard.iter_doc_filepaths(**kwargs):
                yield doc_id, blob

    def pull(self, remote='origin', branch_name='master'):
        with self._index_lock:
            for shard in self._shards:
                shard.pull(remote=remote, branch_name=branch_name)
            self._locked_refresh_doc_ids()

    def report_configuration(self):
        out = StringIO()
        self.write_configuration(out)
        return out.getvalue()

    def write_configuration(self, out, secret_attrs=False):
        """Generic configuration, may be overridden by type-specific version"""
        cd = self.get_configuration_dict(secret_attrs=secret_attrs)
        key_order = list(cd.keys())
        key_order.sort()
        for k in key_order:
            out.write('  {} = {}'.format(k, cd[k]))
        for n, shard in enumerate(self._shards):
            out.write('Shard {}:\n'.format(n))
            shard.write_configuration(out)

    def get_configuration_dict(self, secret_attrs=False):
        """Generic configuration, may be overridden by type-specific version"""
        cd = {'number_of_shards': len(self._shards),
              'initialization': self._filepath_args,
              'shards': [],
              }
        for i in self._shards:
            cd['shards'].append(i.get_configuration_dict(secret_attrs=secret_attrs))
        return cd

    def get_branch_list(self):
        a = []
        for i in self._shards:
            a.extend(i.get_branch_list())
        return a

    def get_changed_docs(self, ancestral_commit_sha, doc_ids_to_check=None):
        ret = None
        for i in self._shards:
            x = i.get_changed_docs(ancestral_commit_sha, doc_ids_to_check=doc_ids_to_check)
            if x is not False:
                ret = x
                break
        if ret is not None:
            return ret
        raise ValueError('No docstore shard returned changed documents for the SHA')

    def get_doc_ids(self):
        k = []
        for shard in self._shards:
            k.extend(shard.get_doc_ids())
        return k

    def is_plausible_transformation(self, subresource_request):
        try:
            return self.doc_schema.is_plausible_transformation_or_raise(subresource_request)
        except ValueError as ve:
            return False, ve.message, None
        except Exception as x:
            return False, str(x), None

    def get_markdown_comment(self, document_obj):
        return ''


class SimpleJSONDocSchema(object):
    """This class implements the is_plausible_transformation_or_raise functionality needed by
    the phylesystem-api for doc stores that hold JSON formats that do not support any subsetting
    or transformation into alternative formats.
    """

    def __init__(self, schema_version=None, document_type='unknown JSON', adaptor_factory=None):
        self.schema_version = schema_version
        self.document_type = document_type
        self.adaptor_factory = adaptor_factory

    def is_plausible_transformation_or_raise(self, subresource_request):
        """This function takes a dict describing a transformation to be applied to a document.
        Returns one of the following tuples:
            (False, REASON_STRING, None) to indicate the transformation of documents from this doc store is impossible,
            (True, None, SYNTAX_STRING) to indicate the documents stored in this store need no transformation, OR
            (True, callable, SYNTAX_STRING) to indicate that the transformation may possible, and if the callable is
                called with a the args:
                    (doc_store_umbrella, doc_id, document object, head_sha_for_doc_obj)
                the transformation will be attempted.
                the callable should raise:
                    a ValueError if the transformation is not possible for the document object supplied, or
                    a KeyError to indicate that the requested part of the document was not found in document
        where SYNTAX_STRING  is 'JSON', 'XML', 'NEXUS'... and
        REASON_STRING is a sentence that describes why the transformation is not possible.

        Used in phylesystem-api, to see if the requested transformation is possible for this type of document.
        and then to accomplish the transformation after the document is fetched. The motivation is to
        avoid holding the lock to the repository too long.

        `subresource_request` can hold the following keys on inut
            * output_format: mapping to a dict which can contain any of the following. all absent -> no transformation
                    {'schema': format name or None,
                              'type_ext': file extension or None
                              'schema_version': default '0.0.0' or the param['output_nexml2json'], }
            * subresource_req_dict['subresource_type'] = string
            * subresource_id = string or (string, string) set of IDs

        The default behavior is to only return:
           - (True, None, "JSON") if no transformation is requested, OR
           - (False, None, None) otherwise.
        The phylesystem umbrella overrides this to allow fetching parts of the document.
        """
        impossible = (False, "No transformations of {} documents are supported.".format(self.document_type), None)
        plausible = (True, None, "JSON")
        if subresource_request.get('subresource_type') or subresource_request.get('subresource_id'):
            return impossible
        out_fmt = subresource_request.get('output_format')
        if not out_fmt:
            return plausible
        schema = out_fmt.get('schema')
        if schema is not None and schema.upper() != 'JSON':
            return impossible
        type_ext = out_fmt.get('type_ext')
        if type_ext is not None and type_ext.upper() != 'JSON':
            return impossible
        return plausible

    def is_valid(self, document, **kwargs):
        return len(self.validate(document, **kwargs)[0]) == 0


    def validate(self, document, **kwargs):
        errors = []
        adaptor = self.adaptor_factory(document, errors, **kwargs)
        return errors, adaptor


    def validate_annotate_convert_doc(self, document, **kwargs):
        """No conversion between different schema is supported for simple types"""
        errors, adaptor = self.validate(document)
        return document, errors, None, adaptor
