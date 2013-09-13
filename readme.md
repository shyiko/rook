# rook [![Build Status](https://travis-ci.org/shyiko/rook.png?branch=master)](https://travis-ci.org/shyiko/rook) [![Coverage Status](https://coveralls.io/repos/shyiko/rook/badge.png?branch=master)](https://coveralls.io/r/shyiko/rook?branch=master)

Change Data Capture (CDC) toolkit for keeping system layers in sync with the database.

Out-of-box rook includes support for MySQL as a source and Hibernate 4 cache (query & 2nd level) as a target.

## Usage

The latest development version always available through Sonatype Snapshots repository:

```xml
<repositories>
    <repository>
    <id>sonatype-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
    <releases>
        <enabled>false</enabled>
    </releases>
    </repository>
</repositories>
```

### Eviction of Hibernate 4 Second Level/Query cache records in response to the replication events (on MySQL)

```xml
<dependencies>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-source-mysql</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </dependency>
    <dependency>
        <groupId>com.github.shyiko.rook</groupId>
        <artifactId>rook-target-hibernate4-cache</artifactId>
        <version>0.1.0-SNAPSHOT</version>
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

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
