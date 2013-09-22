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

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class RowsMutation {

    private final List<Serializable[]> rows;
    private final IndexingDirective indexingDirective;

    public RowsMutation(List<Serializable[]> rows, IndexingDirective indexingDirective) {
        this.rows = rows;
        this.indexingDirective = indexingDirective;
    }

    public List<Serializable[]> getRows() {
        return rows;
    }

    public IndexingDirective getIndexingDirective() {
        return indexingDirective;
    }
}
