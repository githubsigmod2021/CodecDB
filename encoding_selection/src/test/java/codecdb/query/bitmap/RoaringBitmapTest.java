package codecdb.query.bitmap;

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoaringBitmapTest {
    @Test
    public void testGetAndSet() {
        RoaringBitmap bm = new RoaringBitmap();

        Set<Integer> data = new HashSet<>();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            data.add(r.nextInt(10000));
        }

        for (int i : data) {
            bm.set(i, true);
        }
        for (int i : data) {
            assertTrue(bm.test(i));
        }
    }

    @Test
    public void testForeach() {
        RoaringBitmap bm = new RoaringBitmap();

        Set<Integer> data = new HashSet<>();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            data.add(r.nextInt(10000));
        }

        for (int i : data) {
            bm.set(i, true);
        }

        Set<Long> d2 = new HashSet<>();
        bm.foreach((long d) -> {
            d2.add(d);
        });

        assertEquals(d2.size(), data.size());

        for (long ddd : d2) {
            assertTrue(data.contains((int) ddd));
        }
    }
}
