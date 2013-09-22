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
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import java.util.Collections;
import java.util.HashSet;
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
    @LazyCollection(LazyCollectionOption.TRUE)
    @Cascade(org.hibernate.annotations.CascadeType.SAVE_UPDATE)
    @ManyToMany(cascade = {
        CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REFRESH, CascadeType.DETACH
    }, fetch = FetchType.LAZY, mappedBy = "rootEntities")
    private Set<ManyToManyEntity> manyToManyEntities = new HashSet<ManyToManyEntity>();
    @IndexedEmbedded
    @LazyCollection(LazyCollectionOption.TRUE)
    @Cascade(org.hibernate.annotations.CascadeType.SAVE_UPDATE)
    @OneToMany(cascade = {
        CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REFRESH, CascadeType.DETACH
    }, fetch = FetchType.LAZY, mappedBy = "rootEntity")
    private Set<JoinedOneToManyEntity> joinedOneToManyEntities = new HashSet<JoinedOneToManyEntity>();

    public RootEntity() {
    }

    public RootEntity(String name) {
        this.name = name;
    }

    public RootEntity(String name, Set<ManyToManyEntity> manyToManyEntities) {
        this(name, manyToManyEntities, Collections.<JoinedOneToManyEntity>emptySet());
    }

    public RootEntity(String name, Set<ManyToManyEntity> manyToManyEntities,
            Set<JoinedOneToManyEntity> joinedOneToManyEntities) {
        this.name = name;
        for (ManyToManyEntity manyToManyEntity : manyToManyEntities) {
            addManyToManyEntity(manyToManyEntity);
        }
        for (JoinedOneToManyEntity joinedOneToManyEntity : joinedOneToManyEntities) {
            addJoinedOneToManyEntity(joinedOneToManyEntity);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ManyToManyEntity getManyToManyEntityByName(String name) {
        for (ManyToManyEntity oneToManyEntity : this.manyToManyEntities) {
            if (name.equals(oneToManyEntity.getName())) {
                return oneToManyEntity;
            }
        }
        return null;
    }

    public void addManyToManyEntity(ManyToManyEntity manyToManyEntity) {
        this.manyToManyEntities.add(manyToManyEntity);
        manyToManyEntity.addRootEntity(this);
    }

    public void removeManyToManyEntity(ManyToManyEntity manyToManyEntity) {
        this.manyToManyEntities.remove(manyToManyEntity);
        manyToManyEntity.removeRootEntity(this);
    }

    public JoinedOneToManyEntity getJoinedOneToManyEntityByName(String name) {
        for (JoinedOneToManyEntity joinedOneToManyEntity : this.joinedOneToManyEntities) {
            if (name.equals(joinedOneToManyEntity.getName())) {
                return joinedOneToManyEntity;
            }
        }
        return null;
    }

    public void addJoinedOneToManyEntity(JoinedOneToManyEntity joinedOneToManyEntity) {
        this.joinedOneToManyEntities.add(joinedOneToManyEntity);
        joinedOneToManyEntity.setRootEntity(this);
    }

    public void removeJoinedOneToManyEntity(JoinedOneToManyEntity joinedOneToManyEntity) {
        this.joinedOneToManyEntities.remove(joinedOneToManyEntity);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RootEntity student = (RootEntity) o;
        return name.equals(student.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

}
