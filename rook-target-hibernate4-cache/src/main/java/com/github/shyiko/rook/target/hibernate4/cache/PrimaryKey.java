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
package com.github.shyiko.rook.target.hibernate4.cache;

import org.hibernate.mapping.Collection;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Component;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.Table;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class PrimaryKey {

    private Class entityClass;
    private final KeyColumn[] positionWithinRow;

    public PrimaryKey(Collection collection) {
        this(collection.getKey(), collection.getCollectionTable());
        if (positionWithinRow.length != 1) {
            throw new IllegalStateException("Unexpected PK length " + positionWithinRow.length);
        }
    }

    public PrimaryKey(PersistentClass persistentClass) {
        this(persistentClass.getKey(), persistentClass.getTable());
        entityClass = persistentClass.getMappedClass();
    }

    private PrimaryKey(KeyValue keyValue, Table table) {
        Map<String, Integer> columnIndexByName = getColumnIndexByName(table);
        KeyColumn[] positionWithinRow = new KeyColumn[keyValue.getColumnSpan()];
        int index = 0;
        if (keyValue instanceof Component) {
            Component component = (Component) keyValue;
            Iterator propertyIterator = component.getPropertyIterator();
            while (propertyIterator.hasNext()) {
                Property property = (Property) propertyIterator.next();
                Column column = (Column) property.getColumnIterator().next();
                String columnName = column.getName();
                positionWithinRow[index++] = new KeyColumn(property.getName(), columnIndexByName.get(columnName));
            }
        } else {
            Iterator columnIterator = keyValue.getColumnIterator();
            while (columnIterator.hasNext()) {
                Column column = (Column) columnIterator.next();
                String columnName = column.getName();
                positionWithinRow[index++] = new KeyColumn(columnName, columnIndexByName.get(columnName));
            }
        }
        if (positionWithinRow.length == 0) {
            throw new IllegalStateException("Unable to determine PK for " + table.getName());
        }
        this.positionWithinRow = positionWithinRow;
    }

    private Map<String, Integer> getColumnIndexByName(Table table) {
        Map<String, Integer> columnIndexByName = new HashMap<String, Integer>();
        int index = 0;
        @SuppressWarnings("unchecked")
        Iterator<Column> columnIterator = table.getColumnIterator();
        while (columnIterator.hasNext()) {
            Column column = columnIterator.next();
            columnIndexByName.put(column.getName(), index++);
        }
        return columnIndexByName;
    }

    public Serializable getIdentifier(Serializable[] row) {
        if (positionWithinRow.length == 1) {
            return row[positionWithinRow[0].index];
        }
        try {
            Serializable identifier = (Serializable) entityClass.newInstance();
            for (KeyColumn keyColumn : positionWithinRow) {
                Field field = entityClass.getDeclaredField(keyColumn.name);
                field.setAccessible(true);
                field.set(identifier, row[keyColumn.index]);
            }
            return identifier;
        } catch (Throwable e) {
            throw new IllegalStateException("Unable to instantiate entity key", e);
        }
    }

    /**
     * Class that contains single key column data.
     */
    private static final class KeyColumn {

        private final String name;
        private final int index;

        private KeyColumn(String name, int index) {
            this.name = name;
            this.index = index;
        }
    }
}
