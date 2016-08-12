/*
 * Copyright 2016 Igor Grunskiy
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
package com.github.shyiko.rook.it.h4com.model;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;

/**
 * @author <a href="mailto:igor.grunskyi@gmail.com">Igor Grunskiy</a>
 */
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
@javax.persistence.Entity
@IdClass(CompositeKeyEntity.CompositePK.class)
public class CompositeKeyEntity implements Serializable {

    @Id
    @Column(name = "root_id", nullable = false)
    private Long rootId;

    @Id
    @Column(name = "ignored_id", nullable = false)
    private Long ignoredId;

    public CompositeKeyEntity() {
    }

    public CompositeKeyEntity(Long rootId, Long ignoredId) {
        this.ignoredId = ignoredId;
        this.rootId = rootId;
    }

    public Long getIgnoredId() {
        return ignoredId;
    }

    public void setIgnoredId(Long ignoredId) {
        this.ignoredId = ignoredId;
    }

    public Long getRootId() {
        return rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeKeyEntity)) return false;

        CompositeKeyEntity that = (CompositeKeyEntity) o;

        if (rootId != null ? !rootId.equals(that.rootId) : that.rootId != null) {
            return false;
        }
        return !(ignoredId != null ? !ignoredId.equals(that.ignoredId) : that.ignoredId != null);
    }

    @Override
    public int hashCode() {
        int result = rootId != null ? rootId.hashCode() : 0;
        result = 31 * result + (ignoredId != null ? ignoredId.hashCode() : 0);
        return result;
    }

    public static class CompositePK implements Serializable {
        Long rootId;
        Long ignoredId;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositePK)) return false;

            CompositePK that = (CompositePK) o;

            if (!rootId.equals(that.rootId)) return false;
            return ignoredId.equals(that.ignoredId);

        }

        @Override
        public int hashCode() {
            int result = (int) (rootId ^ (rootId >>> 32));
            result = 31 * result + (int) (ignoredId ^ (ignoredId >>> 32));
            return result;
        }
    }

}
