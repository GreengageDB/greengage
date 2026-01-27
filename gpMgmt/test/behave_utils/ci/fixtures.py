from behave import fixture


@fixture
def init_cluster(context):
    if "concourse_cluster" in set(context.config.tags):
        if "concourse_cluster_4" in set(context.feature.tags):
            segments = 4
        elif "concourse_cluster_2" in set(context.feature.tags):
            segments = 2
        else:
            segments = 3
        segments_str = ','.join('sdw{}'.format(i) for i in range(1, segments+1))
        context.execute_steps(u"""
            Given the database is not running
            And a working directory of the test as '/data/gpdata'
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a cluster is created with mirrors on "cdw" and "{}"
            And the user runs "gpconfig -c gp_interconnect_transmit_timeout -v 10s"
            And gpconfig should return a return code of 0
            And the user runs "gpstop -u"
            And gpstop should return a return code of 0
        """.format(segments_str))
    else:
        context.execute_steps(u"""
            Given the database is not running
            And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
            And a standard local demo cluster is created
        """)
