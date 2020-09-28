package codecdb.dataset.feature;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class BloomFilterWrapper {

    public static BloomFilter<Long> create(int expectSize) {
        return BloomFilter.create(Funnels.longFunnel(), expectSize);
    }
}
