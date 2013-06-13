package com.github.shyiko.rook.target.hibernate.cache.model;

import javax.persistence.*;
import java.io.Serializable;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 *         2013-06-12
 */
@Entity
@Table(name = "dummy_entity")
public class DummyEntity implements Serializable{

    @Id
    @GeneratedValue
    @Column
    private Long id;

    @Column
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
