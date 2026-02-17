from behave import fixture


@fixture
def init_cluster(context):
    if "concourse_cluster" in context.scenario.effective_tags:
        if "concourse_cluster_4" in context.feature.tags:
            segment_hosts_in_cluster = 4
        elif "concourse_cluster_2" in context.feature.tags:
            segment_hosts_in_cluster = 2
        else:
            segment_hosts_in_cluster = 3
    else:
        segment_hosts_in_cluster = 0
    if hasattr(context, "segment_hosts_in_cluster"):
        if context.segment_hosts_in_cluster == segment_hosts_in_cluster:
            return
    context.segment_hosts_in_cluster = segment_hosts_in_cluster
    if segment_hosts_in_cluster > 0:
        context.execute_steps(u"""
            Given the database is not running
            And a working directory of the test as '/data/gpdata'
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a cluster is created with mirrors on "cdw" and "{}"
        """.format(','.join('sdw{}'.format(i) for i in range(1, segment_hosts_in_cluster + 1))))
    else:
        context.execute_steps(u"""
            Given the database is not running
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a standard local demo cluster is created
        """)
