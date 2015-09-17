/*
 * Copyright 2015 Igor Grunskiy
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
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * @author <a href="mailto:igor.grunskiy@lifestreetmedia.com">Igor Grunskiy</a>
 */
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
@javax.persistence.Entity
public class IgnoredEntity {
    @Id
    @GeneratedValue
    private long id;
    @Column
    private String name;

    public IgnoredEntity() {
    }

    public IgnoredEntity(String name) {
        this.name = name;
    }
}
