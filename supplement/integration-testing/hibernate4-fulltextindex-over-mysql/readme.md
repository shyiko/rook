# hibernate4-fulltextindex-over-mysql

This module contains number of integration tests asserting Full Text index synchronization (with Hibernate 4 Search)
using replication stream. Test environment (two MySQL nodes connected by means of row-based (master-slave)
replication) is automatically provisioned by [vagrant](http://www.vagrantup.com/).

In order to run tests hit:

    mvn clean verify

> Make sure you have installed [vagrant](http://www.vagrantup.com/) (from [here](http://docs.vagrantup.com/v2/installation/index.html))
and [virtualbox](http://www.virtualbox.org/) (from [here](https://www.virtualbox.org/wiki/Downloads)) before running the line above.
