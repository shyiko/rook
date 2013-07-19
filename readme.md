# rook [![Build Status](https://travis-ci.org/shyiko/rook.png?branch=master)](https://travis-ci.org/shyiko/rook)

Change Data Capture (CDC) toolkit for keeping system layers in sync with the database.

Out-of-box rook includes support for MySQL as a source and Hibernate 4 cache (query & 2nd level) as a target.

Examples
---------------

Automatic eviction of Hibernate 4 Query & Second Level caches in case of MySQL Master-Slave setup<br/>
[supplement/integration-testing/hibernate4-cache-over-mysql](https://github.com/shyiko/rook/tree/master/supplement/integration-testing/hibernate4-cache-over-mysql)

License
---------------

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
