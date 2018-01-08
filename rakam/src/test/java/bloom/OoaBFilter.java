package bloom;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;

import java.math.RoundingMode;

/**
 * OoaBFilter is used to filter out duplicate elements from a given dataset or stream. It is
 * guaranteed to never return a false positive (that is, it will never say that an item has already
 * been seen by the filter when it has not) but may return a false negative.
 * <p>
 * The check is syncronized on the individual buffer. Depending on the dataset, hash function
 * and size of the underlying array lock contention should be very low. Dataset/hash function
 * combinations that cause many collisions will result in more contention.
 * <p>
 * OoaBFilter is thread-safe.
 */
public class OoaBFilter {
    private static final HashFunction HASH_FUNC = Hashing.murmur3_32();
    private final long[] leastSignificantBitsArray;
    private final long[] mostSignificantBitsArray;
    private final int sizeMask;

    public OoaBFilter(int size) {
        leastSignificantBitsArray = new long[size];
        mostSignificantBitsArray = new long[size];

        this.sizeMask = IntMath.pow(2, IntMath.log2(size, RoundingMode.CEILING)) - 1;
    }

    public boolean containsAndAdd(long leastSignificantBits, long mostSignificantBits) {
        HashCode code = HASH_FUNC.hashLong(leastSignificantBits);
        int index = code.asInt() & sizeMask;

        int buffer = (int) leastSignificantBitsArray[index];
        if (mostSignificantBitsArray[buffer] == mostSignificantBits) {
            return true;
        } else {
            mostSignificantBitsArray[buffer] = mostSignificantBits;
            return false;
        }
    }
}