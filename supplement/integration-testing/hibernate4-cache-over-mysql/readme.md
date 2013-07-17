# hibernate4-cache-over-mysql

This module contains number of tests asserting Hibernate cache (query & 2nd level) eviction in response to the replication events.
In order for tests to work two (or more) MySQL nodes need to be connected by means of row-based (master-slave) replication.

The fastest (and the least intrusive) way to set things up is by installing [vagrant](http://www.vagrantup.com/) (from [here](http://docs.vagrantup.com/v2/installation/index.html)), [virtualbox](http://www.virtualbox.org/) (from [here](https://www.virtualbox.org/wiki/Downloads)) and running `vagrant up` within current directory.

The other way is to use [mysql-sandbox](http://mysqlsandbox.net/) as shown below.

    curl -L https://launchpad.net/mysql-sandbox/mysql-sandbox-3/mysql-sandbox-3/+download/MySQL-Sandbox-3.0.33.tar.gz | tar xzv
    cd MySQL-Sandbox-3.0.33
    # follow installation instructions from ./README
    # add "export PATH=$HOME/usr/local/bin:$PATH; export PERL5LIB=$HOME/usr/local/lib/perl5/site_perl" to the ~/.bashrc
    make_replication_sandbox ~/Downloads/mysql-5.5.27-osx10.6-x86_64.tar.gz \
        --how_many_slaves=1 \
        --sandbox_base_port=33061 \
        --master_options='-c binlog_format=ROW' \
        --slave_options='-c binlog_format=ROW -c log-slave-updates=TRUE'
    cd ~/sandboxes/rsandbox_mysql-5_5_27
    ./m -e 'create database rook_h4cs_test; create database rook_h4cs_sdb_test';

Once both master and slave nodes are up, integration tests can be run with

    mvn clean test

