# rook

Change Data Capture (CDC) toolkit for keeping system layers in sync with the database.
General idea is that events from the source are delivered to the target which then decides how to react.
In this way, one could easily implement listener to update Solr index every time changes are made to the database,
or evict memcached entries on the slaves as data gets replicated from the master, or use Percona XtraDB Cluster for
master-master synchronous replication while maintaining up-to-date Hibernate cache no matter what server executed
write statement, etc.
Out-of-box rook includes support for MySQL as a source and Hibernate Cache as a target.

Currently rook-source-mysql contains code of [open-replicator](https://code.google.com/p/open-replicator). This is going to be changed after v0.1.0 release.

Examples
---------------

Automatic eviction of Hibernate Query & Second Level caches in case of MySQL Master-Slave setup (complete version
available at [supplement/integration-testing/hibernate-cache-over-mysql](supplement/integration-testing/hibernate-cache-over-mysql)).

    SessionFactory sessionFactory = ...
    new MySQLReplicationStream("localhost", "3306").
        usingCredentials("username", "password").
        registerListener(new QueryCacheSynchronizer(sessionFactory)).
        registerListener(new SecondLevelCacheSynchronizer(sessionFactory)).
        connect();

License
---------------

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)