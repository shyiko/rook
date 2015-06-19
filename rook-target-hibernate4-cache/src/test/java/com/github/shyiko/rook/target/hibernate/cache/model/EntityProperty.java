package com.github.shyiko.rook.target.hibernate.cache.model;


import javax.persistence.*;
import java.io.Serializable;

/**
 * @author <a href="mailto:igor.grunskiy@lifestreetmedia.com">Igor Grunskiy</a>
 */
@javax.persistence.Entity
@Table(name = "entity_property")
public class EntityProperty implements Serializable {

    @Id
    @GeneratedValue
    private long id;

    @ManyToOne
    @JoinColumn(name = "entity_id")
    private Entity enclosingEntity;

    @Column(name = "name", nullable = false)
    private String name;


    @Column(columnDefinition = "mediumtext", name = "value")
    private String value;

    public Entity getEnclosingEntity() {
        return enclosingEntity;
    }

    public void setEnclosingEntity(Entity enclosingEntity) {
        this.enclosingEntity = enclosingEntity;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
