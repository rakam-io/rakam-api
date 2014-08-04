package org.rakam.cache.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.*;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class LimitPredicate extends Predicates.AbstractPredicate implements IdentifiedDataSerializable {
        protected Integer limit;

        public LimitPredicate() {
        }

        public LimitPredicate(String attribute, Integer limit) {
            super(attribute);
            this.limit = limit;
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            try {
                SortedIndexStore indexStore = (SortedIndexStore) FieldUtils.readField(index, "indexStore", true);
                ConcurrentSkipListSet<Comparable> set = ((ConcurrentSkipListSet<Comparable>) FieldUtils.readField(indexStore, "sortedSet", true));
                ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> map = (ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>>) FieldUtils.readField(indexStore, "mapRecords", true);
                MultiResultSet results = new MultiResultSet();
                Iterator<Comparable> it = set.descendingIterator();
                int i = 0;
                while (i++ < limit && it.hasNext())
                    results.addResultSet(map.get(it.next()));

                return results;
            } catch (IllegalAccessException e) {
                throw new IllegalAccessError("an error occurred.");
            }
        }

        public boolean apply(Map.Entry mapEntry) {
            return true;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeInt(limit);
        }

        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            limit = in.readInt();
        }

        @Override
        public String toString() {
            return attribute + " limit ";
        }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.LIMIT_PREDICATE;
    }
}