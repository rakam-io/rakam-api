package org.rakam.collection.event;

import java.util.Arrays;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/02/15 19:46.
 */
public class SparseArray<E> {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private Object[] elementData;

    public SparseArray() {
        this.elementData = new Object[0];
    }

    private void ensureExplicitCapacity(int minCapacity) {
        // overflow-conscious code
        if (minCapacity - elementData.length >= 0)
            grow(minCapacity+1);
    }

    private void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = elementData.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        // minCapacity is usually close to size, so this is a win:
        elementData = Arrays.copyOf(elementData, newCapacity);
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    public int indexOf(Object o) {
        if (o == null) {
            for (int i = 0; i < elementData.length; i++)
                if (elementData[i]==null)
                    return i;
        } else {
            for (int i = 0; i < elementData.length; i++)
                if (o.equals(elementData[i]))
                    return i;
        }
        return -1;
    }

    public E get(int index) {
        rangeCheck(index);

        return elementData(index);
    }

    private void rangeCheck(int index) {
        if (index >= elementData.length)
            throw new IndexOutOfBoundsException("Index: "+index+", Size: "+elementData.length);
    }

    @SuppressWarnings("unchecked")
    E elementData(int index) {
        return (E) elementData[index];
    }

    public E set(int index, E element) {
        ensureExplicitCapacity(index);

        E oldValue = elementData(index);
        elementData[index] = element;
        return oldValue;
    }

    public int size() {
        return elementData.length;
    }
}
