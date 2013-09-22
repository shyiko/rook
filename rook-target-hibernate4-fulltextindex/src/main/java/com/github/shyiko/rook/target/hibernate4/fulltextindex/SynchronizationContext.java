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
package com.github.shyiko.rook.target.hibernate4.fulltextindex;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.ToOne;
import org.hibernate.mapping.Value;
import org.hibernate.property.Getter;
import org.hibernate.search.annotations.ContainedIn;
import org.hibernate.search.annotations.Indexed;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SynchronizationContext {

    private final String schema;
    private final SessionFactory sessionFactory;

    private final Map<String, IndexingDirective> directivesByTable = new HashMap<String, IndexingDirective>();
    private final Map<Class, IndexingDirective> directivesByEntityClass = new HashMap<Class, IndexingDirective>();

    public SynchronizationContext(Configuration configuration, SessionFactory sessionFactory) {
        this.schema = ((SessionFactoryImplementor) sessionFactory).getJdbcServices().
            getExtractedMetaDataSupport().getConnectionCatalogName().toLowerCase();
        this.sessionFactory = sessionFactory;
        loadIndexingDirectives(configuration);
    }

    public SessionFactoryImplementor getSessionFactory() {
        return (SessionFactoryImplementor) sessionFactory;
    }

    public Collection<IndexingDirective> getIndexingDirectives(String table) {
        IndexingDirective indexingTarget = directivesByTable.get(table.toLowerCase());
        return indexingTarget == null ? Collections.<IndexingDirective>emptyList() : Arrays.asList(indexingTarget);
    }

    public IndexingDirective getIndexingDirective(Class entityClass) {
        return directivesByEntityClass.get(entityClass);
    }

    private void loadIndexingDirectives(Configuration configuration) {
        Map<String, IndexingDirective> directivesByEntityNameMap = new HashMap<String, IndexingDirective>();
        Collection<Property> allContainedInProperties = new ArrayList<Property>();
        for (Iterator<PersistentClass> classIterator = configuration.getClassMappings(); classIterator.hasNext(); ) {
            PersistentClass persistentClass = classIterator.next();
            boolean suppressSelfIndexing = true;
            Class mappedClass = persistentClass.getMappedClass();
            if (mappedClass.isAnnotationPresent(Indexed.class)) {
                suppressSelfIndexing = false;
            }
            Collection<Property> containedInProperties = extractAnnotatedProperties(persistentClass, ContainedIn.class);
            if (suppressSelfIndexing && containedInProperties.isEmpty()) {
                continue;
            }
            allContainedInProperties.addAll(containedInProperties);
            PrimaryKey primaryKey = new PrimaryKey(persistentClass);
            Collection<Reference> containers = new ArrayList<Reference>();
            for (Property property : containedInProperties) {
                containers.add(new Reference(property.getGetter(mappedClass)));
            }
            IndexingDirective indexingDirective = new IndexingDirective(primaryKey, suppressSelfIndexing, containers);
            Table table = persistentClass.getTable();
            directivesByTable.put(schema + "." + table.getName().toLowerCase(), indexingDirective);
            directivesByEntityClass.put(mappedClass, indexingDirective);
            directivesByEntityNameMap.put(persistentClass.getEntityName(), indexingDirective);
        }
        loadIndexingDirectivesForJoinTables(allContainedInProperties, directivesByEntityNameMap);
    }

    @SuppressWarnings("unchecked")
    private Collection<Property> extractAnnotatedProperties(PersistentClass persistentClass,
                                                            Class<? extends Annotation> annotation) {
        Class mappedClass = persistentClass.getMappedClass();
        Collection<Property> properties = new ArrayList<Property>();
        for (Iterator<Property> propertyIterator = persistentClass.getPropertyIterator();
             propertyIterator.hasNext(); ) {
            Property property = propertyIterator.next();
            Getter getter = property.getGetter(mappedClass);
            if (getter == null) {
                continue;
            }
            Member mappedClassMember = getter.getMember();
            boolean isRequestedAnnotationPresent = mappedClassMember instanceof AccessibleObject &&
                ((AccessibleObject) mappedClassMember).isAnnotationPresent(annotation);
            if (isRequestedAnnotationPresent) {
                properties.add(property);
            }
        }
        return properties;
    }

    private void loadIndexingDirectivesForJoinTables(Collection<Property> allContainedInProperties,
            Map<String, IndexingDirective> directivesByEntityNameMap) {
        for (Property property : allContainedInProperties) {
            Value value = property.getValue();
            if (value instanceof org.hibernate.mapping.Collection) {
                org.hibernate.mapping.Collection collection = (org.hibernate.mapping.Collection) value;
                Table collectionTable = collection.getCollectionTable();
                String tableName = schema + "." + collectionTable.getName().toLowerCase();
                if (directivesByTable.containsKey(tableName)) {
                    continue;
                }
                PrimaryKey primaryKey = resolveForeignPrimaryKey(collection, directivesByEntityNameMap);
                if (primaryKey == null) {
                    continue;
                }
                IndexingDirective containerIndexingDirective = directivesByEntityClass.get(primaryKey.getEntityClass());
                directivesByTable.put(tableName,
                    new IndexingDirective(primaryKey, containerIndexingDirective.isSuppressSelfIndexing(),
                        containerIndexingDirective.getContainerReferences()));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private PrimaryKey resolveForeignPrimaryKey(org.hibernate.mapping.Collection collection,
            Map<String, IndexingDirective> directivesByEntityNameMap) {
        Table collectionTable = collection.getCollectionTable();
        ToOne element = (ToOne) collection.getElement();
        IndexingDirective indexingDirective = directivesByEntityNameMap.get(element.getReferencedEntityName());
        if (indexingDirective == null) {
            return null;
        }
        Collection<String> targetPrimaryKeyColumnNames = new HashSet<String>();
        for (Iterator<Column> columnIterator = element.getColumnIterator(); columnIterator.hasNext(); ) {
            Column column = columnIterator.next();
            targetPrimaryKeyColumnNames.add(column.getName());
        }
        Map<String, Integer> columnIndexByNameMap = new HashMap<String, Integer>();
        int index = 0;
        for (Iterator<Column> columnIterator = collectionTable.getColumnIterator(); columnIterator.hasNext(); ) {
            Column column = columnIterator.next();
            if (targetPrimaryKeyColumnNames.contains(column.getName())) {
                columnIndexByNameMap.put(column.getName(), index);
            }
            index++;
        }
        return new PrimaryKey(indexingDirective.getPrimaryKey(), columnIndexByNameMap);
    }

}
