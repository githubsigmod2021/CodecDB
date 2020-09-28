package codecdb.query.bitmap;

import java.util.function.LongConsumer;

public interface Bitmap {

    void set(long index, boolean value);

    boolean test(long index);

    void foreach(LongConsumer consumer);

    Bitmap and(Bitmap another);

    Bitmap or(Bitmap another);
}
