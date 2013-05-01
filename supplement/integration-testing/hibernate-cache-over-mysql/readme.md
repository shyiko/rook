# hibernate-cache-over-mysql

In order for tests to work you'll need to set up MySQL Master-Slave row-based replication of "rook_h4cs_test" database.
The easiest way is through [mysql-sandbox](http://mysqlsandbox.net/) as shown next.

    curl -L https://launchpad.net/mysql-sandbox/mysql-sandbox-3/mysql-sandbox-3/+download/MySQL-Sandbox-3.0.33.tar.gz | tar xv
    cd MySQL-Sandbox-3.0.33
    # follow installation instructions from ./README
    # in case of root-less option, don't forget to add "export PATH=$HOME/usr/local/bin:$PATH; export PERL5LIB=$HOME/usr/local/lib/perl5/site_perl" to the ~/.bashrc
    make_replication_sandbox ~/Downloads/mysql-5.5.27-osx10.6-x86_64.tar.gz \
        --how_many_slaves=1 \
        --sandbox_base_port=33061 \
        --master_options='-c binlog_format=ROW' \
        --slave_options='-c binlog_format=ROW -c log-slave-updates=TRUE'
    cd ~/sandboxes/rsandbox_mysql-5_5_27
    ./m -e 'create database rook_h4cs_test';

At this point you should be able to run integration tests with

    mvn clean test

