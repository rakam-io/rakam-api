package org.rakam.analysis;

import java.util.Optional;

public class CalculatedUserSet {
    public final Optional<String> collection;
    public final Optional<String> dimension;

    public CalculatedUserSet(Optional<String> collection, Optional<String> dimension) {
        this.collection = collection;
        this.dimension = dimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CalculatedUserSet)) return false;

        CalculatedUserSet that = (CalculatedUserSet) o;

        if (collection != null ? !collection.equals(that.collection) : that.collection != null) return false;
        return !(dimension != null ? !dimension.equals(that.dimension) : that.dimension != null);

    }

    @Override
    public int hashCode() {
        int result = collection != null ? collection.hashCode() : 0;
        result = 31 * result + (dimension != null ? dimension.hashCode() : 0);
        return result;
    }
}
