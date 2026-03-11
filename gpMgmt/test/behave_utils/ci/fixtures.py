from behave import fixture
from test.behave_utils.utils import is_concourse_cluster


@fixture
def init_cluster(context):
    if is_concourse_cluster(context):
        if "concourse_cluster_4" in context.feature.tags:
            segment_hosts_in_cluster = 4
        elif "concourse_cluster_2" in context.feature.tags:
            segment_hosts_in_cluster = 2
        else:
            segment_hosts_in_cluster = 3
    else:
        segment_hosts_in_cluster = 0
    if segment_hosts_in_cluster > 0:
        context.execute_steps(u"""
            Given the database is not running
            And a working directory of the test as '/data/gpdata'
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a cluster is created with mirrors on "cdw" and "{}" from fixture
        """.format(','.join('sdw{}'.format(i + 1) for i in range(segment_hosts_in_cluster))))
    else:
        context.execute_steps(u"""
            Given the database is not running
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a standard local demo cluster is created
        """)
