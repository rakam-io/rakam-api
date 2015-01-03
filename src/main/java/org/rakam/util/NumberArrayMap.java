package org.rakam.util;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/12/14 01:34.
 */
public abstract class NumberArrayMap<K extends Number, V> extends AbstractMap<K, V> implements Map<K, V>, Serializable {

    private static final long serialVersionUID = -2304239764179123L;

    private int minBound;
    private int maxBound;
    private transient int size;
    private transient int modCount;
    private transient int capacity;
    private transient V[] values;

    private transient Set<Entry<K, V>> entrySet;

    protected NumberArrayMap(int minBound, int maxBound) {
        if (minBound > maxBound) {
            throw new IllegalArgumentException("Error: minBound (" + minBound + ") must not be greater than maxBound (" + maxBound + ")");
        }
        this.minBound = minBound;
        this.maxBound = maxBound;
        init();
    }

    private void init() {
        initCapacity();
        clear();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof Number) {
            return containsKey(((Number) key).intValue());
        }
        return false;
    }

    public boolean containsKey(int key) {
        return isInRange(key) && values[index(key)] != null;
    }

    @Override
    public boolean containsValue(Object value) {
        for (final V v : values) {
            if (v != null && v.equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        if ((key instanceof Number)) {
            return get(((Number) key).intValue());
        }
        return null;
    }

    public V get(int key) {
        if (isInRange(key)) {
            return values[index(key)];
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        return put(key.intValue(), value);
    }

    public V put(int key, V value) {
        assertInRange(key);
        Objects.requireNonNull(value, "This Map does not support null values");
        final V oldValue = get(key);
        if (oldValue == null) {
            // A new value, increment size
            size++;
        }
        values[index(key)] = value;
        modCount++;
        return oldValue;
    }

    @Override
    public V remove(Object key) {
        if (key instanceof Number) {
            return remove(((Number) key).intValue());
        }
        return null;
    }

    public V remove(int key) {
        final V oldValue = get(key);
        if (oldValue != null) {
            // An existing value, decrement size
            size--;
        }
        values[index(key)] = null;
        modCount++;
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        @SuppressWarnings("unchecked")
        final V[] newValues = (V[]) new Object[capacity];
        this.values = newValues;
        size = 0;
        modCount++;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> es;
        return (es = entrySet) == null ? (entrySet = new NumberArrayMap<K, V>.EntrySet()) : es;
    }

    final class EntrySet extends AbstractSet<Entry<K, V>> {

        @Override
        public final int size() {
            return size;
        }

        @Override
        public final void clear() {
            NumberArrayMap.this.clear();
        }

        @Override
        public final Iterator<Entry<K, V>> iterator() {
            return new NumberArrayMap<K, V>.EntryIterator();
        }

        @Override
        public final boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            final Object key = e.getKey();
            final V valueCandidate = get(key);
            return valueCandidate != null && valueCandidate.equals(e.getValue());
        }

        @Override
        public final boolean remove(Object o) {
            if (o instanceof Map.Entry) {
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                final Object key = e.getKey();
                final V valueCandidate = get(key);
                if (valueCandidate != null && valueCandidate.equals(e.getValue())) {
                    return NumberArrayMap.this.remove(key) != null;
                }
            }
            return false;
        }

        @Override
        public final void forEach(Consumer<? super Entry<K, V>> action) {
            Objects.requireNonNull(action);
            if (size > 0) {
                int mc = modCount;
                for (int i = minBound; i <= maxBound; ++i) {
                    final V value = get(i);
                    if (value != null) {
                        action.accept(makeEntry(makeKeyFromInt(i), value));
                    }
                    if (modCount != mc) {
                        throw new ConcurrentModificationException();
                    }
                }
            }
        }
    }

    private abstract class BucketIterator<N> implements Iterator<N> {

        private Entry<K, V> next;
        private Entry<K, V> current;
        private int expectedModCount;
        private int currentIndex;

        public BucketIterator() {
            expectedModCount = modCount;
            currentIndex = 0;
            if (size > 0) {
                // advance to first entry
                forwardToNext(minBound);
            }
        }

        @Override
        public final boolean hasNext() {
            return next != null;
        }

        public final Entry<K, V> nextEntry() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (next == null) {
                throw new NoSuchElementException();
            }
            current = next;
            forwardToNext(currentIndex + 1);
            return current;
        }

        @Override
        public final void remove() {
            final Entry<K, V> p = current;
            if (p == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            current = null;
            final K key = p.getKey();
            NumberArrayMap.this.remove(key);
            expectedModCount = modCount;
        }

        private void forwardToNext(int start) {
            if (start <= maxBound) {
                for (int i = start; i <= maxBound; ++i) {
                    final V value = get(i);
                    if (value != null) {
                        currentIndex = i;
                        next = makeEntry(makeKeyFromInt(i), value);
                        return;
                    }
                }
            }
            // We have reached the end...
            next = null;
        }

    }

    final class KeyIterator extends BucketIterator<K> {

        @Override
        public final K next() {
            return nextEntry().getKey();
        }
    }

    final class ValueIterator extends BucketIterator<V> {

        @Override
        public final V next() {
            return nextEntry().getValue();
        }
    }

    final class EntryIterator extends BucketIterator<Map.Entry<K, V>> {

        @Override
        public final Map.Entry<K, V> next() {
            return nextEntry();
        }
    }

    protected abstract K makeKeyFromInt(int k);

    private Entry<K, V> makeEntry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private int index(int key) {
        return key - minBound;
    }

    private boolean isInRange(int key) {
        return (key >= minBound && key <= maxBound);
    }

    private void assertInRange(int key) {
        if (!isInRange(key)) {
            throw new ArrayIndexOutOfBoundsException("Key " + key + " out of range. Key shoud be >= " + minBound + " and <= " + maxBound);
        }
    }

    private void initCapacity() {
        this.capacity = maxBound - minBound + 1;
    }

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeInt(minBound);
        s.writeInt(maxBound);
        s.writeInt(size);
        for (final Entry<K, V> e : entrySet()) {
            final V value = e.getValue();
            if (value != null) {
                s.writeObject(e.getKey());
                s.writeObject(e.getValue());
            }
        }
    }

    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        minBound = s.readInt();
        maxBound = s.readInt();
        init();
        final int noItems = s.readInt();
        if (noItems < 0) {
            throw new InvalidObjectException("Illegal noItems count: " + noItems);
        }
        if (noItems > 0) {
            // Read the keys and values, and put the mappings in the NumberMap
            for (int i = 0; i < noItems; i++) {
                @SuppressWarnings("unchecked")
                final K key = (K) s.readObject();
                @SuppressWarnings("unchecked")
                final V value = (V) s.readObject();
                put(key, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object clone() throws CloneNotSupportedException {
        NumberArrayMap<K, V> result;
        try {
            result = (NumberArrayMap<K, V>) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e);
        }
        result.init();
        result.putAll(this);
        return result;
    }


}

