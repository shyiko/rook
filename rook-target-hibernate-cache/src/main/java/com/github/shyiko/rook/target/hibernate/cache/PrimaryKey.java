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
package com.github.shyiko.rook.target.hibernate.cache;

import org.hibernate.mapping.Collection;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Selectable;
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

    private Class entityClass = null;
    private final KeyColumn[] positionWithinRow;

    private Map<String, Integer> asIndexMap(Table table) {
        Map<String, Integer> columnIndexByName = new HashMap<String, Integer>();
        int index = 0;
        for (@SuppressWarnings("unchecked") Iterator<Column> columnIterator = table.getColumnIterator();
             columnIterator.hasNext(); ) {
            Column column = columnIterator.next();
            columnIndexByName.put(column.getName(), index++);
        }
        return columnIndexByName;
    }

    public PrimaryKey(Collection collection) {
        this(collection.getKey(), collection.getCollectionTable());
    }

    public PrimaryKey(PersistentClass persistentClass) {
        this(persistentClass.getKey(), persistentClass.getTable());
        entityClass = persistentClass.getMappedClass();
    }

    private PrimaryKey(KeyValue keyValue, Table table) {
        Map<String, Integer> indexMap = asIndexMap(table);
        KeyColumn[] pkIndexes = new KeyColumn[keyValue.getColumnSpan()];
        int index = 0;
        for (@SuppressWarnings("unchecked") Iterator<Selectable> columnIterator = keyValue.getColumnIterator();
             columnIterator.hasNext(); ) {
            Column column = (Column) columnIterator.next(); // todo: what if it's Formula?
            pkIndexes[index++] =
                    new KeyColumn(column.getName(), indexMap.get(column.getName()));
        }
        this.positionWithinRow = pkIndexes;
    }

    /**
     * Get identifier for given row values
     *
     * @param row values
     * @return Serializable key
     */
    public Serializable getIdentifier(Serializable[] row) {
        if (positionWithinRow.length == 1) {
            return row[positionWithinRow[0].index];
        } else if (positionWithinRow.length > 1) {
            if (entityClass == null) {
                throw new IllegalStateException("Entity class is undefined! Cannot create key.");
            }
            try {
                Serializable key = (Serializable) entityClass.newInstance();
                for (KeyColumn keyColumn : positionWithinRow) {
                    Field field = entityClass.getDeclaredField(keyColumn.name);
                    field.setAccessible(true);
                    field.set(key, row[keyColumn.index]);
                }
                return key;
            } catch (Throwable e) {
                throw new IllegalStateException("Cannot instantiate entity key", e);
            }
        } else {
            throw new IllegalStateException("There are zero key columns in given key");
        }
    }

    /**
     * Class that contains single key column data
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
