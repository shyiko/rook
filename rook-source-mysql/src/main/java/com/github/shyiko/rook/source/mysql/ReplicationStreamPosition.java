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
package com.github.shyiko.rook.source.mysql;

import org.apache.commons.lang.Validate;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class ReplicationStreamPosition {

    private String binLogFileName;
    private long binLogPosition;

    public ReplicationStreamPosition(String binLogFileName, long binLogPosition) {
        Validate.notNull(binLogFileName, "BinLogFileName cannot be null");
        this.binLogFileName = binLogFileName;
        this.binLogPosition = binLogPosition;
    }

    public String getBinLogFileName() {
        return binLogFileName;
    }

    public long getBinLogPosition() {
        return binLogPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicationStreamPosition that = (ReplicationStreamPosition) o;
        return binLogPosition == that.binLogPosition && binLogFileName.equals(that.binLogFileName);
    }

    @Override
    public int hashCode() {
        int result = binLogFileName.hashCode();
        result = 31 * result + (int) (binLogPosition ^ (binLogPosition >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return binLogFileName + "#" + binLogPosition;
    }
}
