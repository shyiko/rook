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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
