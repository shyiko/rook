# hibernate4-cache-over-mysql

This module contains number of integration tests asserting Hibernate cache (2nd level & query) eviction in response to
the replication events. Test environment (two MySQL nodes connected by means of row-based (master-slave) replication)
is automatically provisioned by [vagrant](http://www.vagrantup.com/).

In order to run tests hit:

    mvn clean verify

> Make sure you have installed [vagrant](http://www.vagrantup.com/) (from [here](http://docs.vagrantup.com/v2/installation/index.html))
and [virtualbox](http://www.virtualbox.org/) (from [here](https://www.virtualbox.org/wiki/Downloads)) before running the line above.
