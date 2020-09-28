package codecdb.model;

import org.apache.parquet.column.Encoding;

/**
 * Created by harper on 4/17/17.
 */
public enum LongEncoding {
    PLAIN(Encoding.PLAIN), DICT(Encoding.PLAIN_DICTIONARY), DELTABP(Encoding.DELTA_BINARY_PACKED),
    BITVECTOR(null);

    private Encoding parquetEncoding;

    LongEncoding(Encoding enc) {
        this.parquetEncoding = enc;
    }

    public Encoding parquetEncoding() {
        return this.parquetEncoding;
    }
}
