"""Base classes for managing sharded document stores in git"""


class GitShardFilepathMapper(object):

    def __init__(self, primary_doc_tag, doc_holder_subpath_list=None):
        self.path_to_user_splitter = ''
        self.wip_id_template = r'.*_' + primary_doc_tag + r'_{i}_[0-9]+'
        self.branch_name_template = "{ghu}_" + primary_doc_tag + "_{rid}"
        self.path_to_user_splitter = "_{}_".format(primary_doc_tag)
        if doc_holder_subpath_list is None:
            self.doc_holder_subpath_list = (primary_doc_tag,)
        else:
            self.doc_holder_subpath_list = doc_holder_subpath_list
        self.doc_parent_dir_list = tuple([i + '/' for i in self.doc_holder_subpath_list])

    def split_branch_to_user(self, branch_name):
        return branch_name.split(self.path_to_user_splitter)[0]

    def id_from_rel_path(self, path):
        for dpd in self.doc_parent_dir_list:
            if path.startswith(dpd):
                p = path.split(dpd)[1]
                if p.endswith('.json'):
                    return p[:-5]
                return p

    def filepath_for_id(self, repo_dir, doc_id):
        assert bool(self.id_pattern.match(doc_id))
        return '{r}/{d}/{s}.json'.format(r=repo_dir, d=self.doc_holder_subpath_list[0], s=doc_id)


# noinspection PyPep8
from peyotl.git_storage.helper import get_phylesystem_repo_parent, get_repos, \
    get_doc_store_repo_parent
from peyotl.git_storage.sharded_doc_store import ShardedDocStore, ShardedDocStoreProxy
from peyotl.git_storage.type_aware_doc_store import TypeAwareDocStore
from peyotl.git_storage.git_action import GitActionBase, RepoLock, MergeException
from peyotl.git_storage.git_workflow import GitWorkflowBase
from peyotl.git_storage.git_shard import GitShard, TypeAwareGitShard


class NonAnnotatingDocValidationAdaptor(object):
    """The write operations for our git_storage adaptors separate an annotation-generation
    step from the adding of annotation to documents.

    This was primarily done to support NexSON documents which lead to some warnings to users that are
    not added to the file, and some annotations that are added to the document (e.g. supplying
    explicit default values for optional properties that were omitted by the client.

    Document types that do not include annotations, just implement a no-op for the annotation-addition hook.
    """

    def add_or_replace_annotation(self,  # pylint: disable=R0201
                                  document,
                                  annotation_obj,
                                  add_agent_only=False):
        pass
