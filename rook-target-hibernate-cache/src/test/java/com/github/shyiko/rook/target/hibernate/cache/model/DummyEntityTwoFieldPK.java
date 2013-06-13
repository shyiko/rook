package com.github.shyiko.rook.target.hibernate.cache.model;

import javax.persistence.*;
import java.io.Serializable;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 *         2013-06-12
 */
@Entity
@Table(name = "dummy_entity_2fpk")
public class DummyEntityTwoFieldPK implements Serializable{

    @Id
    @GeneratedValue
    @Column
    private Long id;

    @Column
    private String name;

    @Id
    @GeneratedValue
    @Column
    private Long id2;


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

    public Long getId2() {
        return id2;
    }

    public void setId2(Long id2) {
        this.id2 = id2;
    }
}
