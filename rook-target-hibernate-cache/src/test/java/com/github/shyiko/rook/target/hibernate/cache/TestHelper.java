/*
 * Copyright 2013 Ivan Zaytsev
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

import org.hibernate.cfg.Configuration;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 *         2013-06-13
 */
final class TestHelper {
    private TestHelper() {
    }

    public static Serializable[] mapColumnValues(Configuration configuration, String entityName,
                                                 Map<String, Serializable> columnValuesByName) {
        Table table = configuration.getClassMapping(entityName).getTable();
        List<Serializable> result = new ArrayList<Serializable>();
        for (@SuppressWarnings("unchecked") Iterator<Column> iterator = table.getColumnIterator();
             iterator.hasNext(); ) {
            Column column = iterator.next();
            if (columnValuesByName.containsKey(column.getName())) {
                result.add(columnValuesByName.get(column.getName()));
            }
        }
        return result.toArray(new Serializable[result.size()]);
    }
}
