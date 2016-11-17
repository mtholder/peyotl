"""Base classes for managing sharded document stores in git"""
from peyotl.git_storage.helper import get_phylesystem_parent_list, get_repos
from peyotl.git_storage.sharded_doc_store import ShardedDocStore
from peyotl.git_storage.type_aware_doc_store import TypeAwareDocStore
from peyotl.git_storage.git_action import GitActionBase, RepoLock
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

