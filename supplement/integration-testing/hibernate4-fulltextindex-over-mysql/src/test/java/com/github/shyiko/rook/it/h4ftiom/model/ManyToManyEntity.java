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

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;
import org.hibernate.search.annotations.ContainedIn;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
@Indexed
@javax.persistence.Entity
public class ManyToManyEntity {

    @Id
    @GeneratedValue
    private long id;
    @Field
    @Column(nullable = false)
    private String name;
    @ContainedIn
    @LazyCollection(LazyCollectionOption.TRUE)
    @ManyToMany(fetch = FetchType.LAZY)
    private Set<RootEntity> rootEntities = new HashSet<RootEntity>();

    public ManyToManyEntity() {
    }

    public ManyToManyEntity(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<RootEntity> getRootEntities() {
        return rootEntities;
    }

    public void addRootEntity(RootEntity rootEntity) {
        this.rootEntities.add(rootEntity);
    }

    public void removeRootEntity(RootEntity rootEntity) {
        this.rootEntities.remove(rootEntity);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManyToManyEntity student = (ManyToManyEntity) o;
        return name.equals(student.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
