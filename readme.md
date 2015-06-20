# rook [![Build Status](https://travis-ci.org/shyiko/rook.png?branch=master)](https://travis-ci.org/shyiko/rook) [![Coverage Status](https://coveralls.io/repos/shyiko/rook/badge.png?branch=master)](https://coveralls.io/r/shyiko/rook?branch=master)

Change Data Capture (CDC) toolkit for keeping system layers in sync with the database.

Out-of-box rook includes support for MySQL as a source and Hibernate 4 cache (2nd Level & Query),
FullText index backed by [Hibernate Search](http://www.hibernate.org/subprojects/search.html) as targets.

## Usage

The latest release version available through [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.shyiko.rook%22%20AND%20a%3A%22rook%22).

### Eviction of Hibernate 4 Second Level/Query cache records in response to the replication events (on MySQL)

```xml
<dependencies>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-source-mysql</artifactId>
        <version>0.1.0</version>
    </dependency>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-target-hibernate4-cache</artifactId>
        <version>0.1.0</version>
    </dependency>
</dependencies>
```

```java
org.hibernate.cfg.Configuration configuration = ...
org.hibernate.SessionFactory sessionFactory = ...
new MySQLReplicationStream("hostname", 3306, "username", "password").
    registerListener(new HibernateCacheSynchronizer(configuration, sessionFactory)).
    connect();
```

> Integration tests available at [supplement/integration-testing/hibernate4-cache-over-mysql](https://github.com/shyiko/rook/tree/master/supplement/integration-testing/hibernate4-cache-over-mysql)

### Update of Hibernate 4 Search controlled FT index in response to the replication events (on MySQL)

> Keep in mind that default indexer, which is used by FullTextIndexSynchronizer, relies on
@org.hibernate.search.annotations.ContainedIn for propagation of indexing events to the container entity(ies).
As a result, either each @IndexedEmbedded-annotated field/method MUST have corresponding @ContainedIn member
(inside target entity) or a different indexer is required.

```xml
<dependencies>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-source-mysql</artifactId>
        <version>0.1.0</version>
    </dependency>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-target-hibernate4-fulltextindex</artifactId>
        <version>0.1.0</version>
    </dependency>
</dependencies>
```

```java
org.hibernate.cfg.Configuration configuration = ...
org.hibernate.SessionFactory sessionFactory = ...
new MySQLReplicationStream("hostname", 3306, "username", "password").
    registerListener(new FullTextIndexSynchronizer(configuration, sessionFactory)).
    connect();
```

> Integration tests available at [supplement/integration-testing/hibernate4-fulltextindex-over-mysql](https://github.com/shyiko/rook/tree/master/supplement/integration-testing/hibernate4-fulltextindex-over-mysql)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
