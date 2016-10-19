#!/bin/bash
source tests/bash-test-helpers.bash || exit
demand_at_top_level || exit

if ! test -d peyotl/test/data/mini_par/mini_phyl
then
    echo "skipping tests against local phylesystem due to lack of mini_phyl (this is normal if you are not a peyotl maintainer)"
    exit 0
fi
if ! test -d peyotl/test/data/mini_par/mini_system
then
    echo "skipping tests against local phylesystem due to lack of mini_system (this is normal if you are not a peyotl maintainer)"
    exit 0
fi
echo "Running tests of the local (mini) phylesystem"


num_fails=0
num_checks=0
refresh_and_test_local_git tests/local_repos_tests/test_caching.py
refresh_and_test_local_git tests/local_repos_tests/test_git_workflows.py
refresh_and_test_local_git tests/local_repos_tests/test_study_del.py
refresh_and_test_local_git tests/local_repos_tests/test_git_workflows.py tiny_max_file_size
refresh_and_test_local_git tests/local_repos_tests/test_phylesystem_api.py

# The following test needs to have mirrors created as a part of the setup
refresh_and_test_local_git_with_mirrors tests/local_repos_tests/test_reduce_dup_doc_store.py

# This test uses the (deprecated) feature of creating push mirrors when wrapping
#   unmirrored dirs. The clean up steps at the bottom of this script require that
#   the mirrors exist. So we either need to keep this test LAST! or add an invocation
#   of a (as-yet-unwritten) script to create the mirrors.
refresh_and_test_local_git tests/local_repos_tests/test_phylesystem_mirror.py

# This resets the head on the remote. A dangerous operation, but this is just a testing repo...
cd peyotl/test/data/mini_par/mirror/mini_phyl
git push -f GitHubRemote baa76d4af8d197107b3ee6f81d45e1fd41b2c4b9:master
cd - >/dev/null 2>&1

exit ${num_fails}
