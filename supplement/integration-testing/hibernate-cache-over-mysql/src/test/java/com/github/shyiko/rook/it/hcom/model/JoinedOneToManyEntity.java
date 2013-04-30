package com.github.shyiko.rook.it.hcom.model;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.LazyToOne;
import org.hibernate.annotations.LazyToOneOption;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
@Entity
public class JoinedOneToManyEntity {

    @Id
    @GeneratedValue
    private long id;
    @Column(nullable = false)
    private String name;
    @ManyToOne(fetch = FetchType.LAZY)
    @LazyToOne(LazyToOneOption.PROXY)
    @JoinColumn(name = "root_entity_id")
    private RootEntity rootEntity;

    public JoinedOneToManyEntity(String name) {
        this.name = name;
    }

    public RootEntity getRootEntity() {
        return rootEntity;
    }

    public void setRootEntity(RootEntity rootEntity) {
        this.rootEntity = rootEntity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinedOneToManyEntity that = (JoinedOneToManyEntity) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
