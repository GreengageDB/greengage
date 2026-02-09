from behave import fixture


@fixture
def init_cluster(context):
    context.execute_steps(u"""
        Given the database is not running
        And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
        And a standard local demo cluster is created
    """)
