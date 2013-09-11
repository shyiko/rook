/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.rook.it.h4ftiom.model;

import org.hibernate.annotations.Cascade;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import java.util.Set;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
@Indexed
@javax.persistence.Entity
public class RootEntity {

    @Id
    @GeneratedValue
    private long id;
    @Field
    @Column
    private String name;
    @IndexedEmbedded
    @Cascade(org.hibernate.annotations.CascadeType.SAVE_UPDATE)
    @OneToMany(cascade = {
        CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REFRESH, CascadeType.DETACH
    }, fetch = FetchType.LAZY)
    private Set<OneToManyEntity> oneToManyEntities;

    public RootEntity() {
    }

    public RootEntity(String name) {
        this.name = name;
    }

    public RootEntity(String name, Set<OneToManyEntity> oneToManyEntities) {
        this.name = name;
        this.oneToManyEntities = oneToManyEntities;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<OneToManyEntity> getOneToManyEntities() {
        return oneToManyEntities;
    }

    public void setOneToManyEntities(Set<OneToManyEntity> oneToManyEntities) {
        this.oneToManyEntities = oneToManyEntities;
    }
}
