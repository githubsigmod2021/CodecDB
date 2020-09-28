package codecdb.parquet;

import org.apache.parquet.column.Encoding;

import java.util.HashMap;
import java.util.Map;

public class EncContext {
    public static final ThreadLocal<Map<String, Object[]>> context = new ThreadLocal<Map<String, Object[]>>() {
        @Override
        protected Map<String, Object[]> initialValue() {
            return new HashMap<String, Object[]>();
        }
    };
    public static final ThreadLocal<Map<String, Encoding>> encoding = new ThreadLocal<Map<String, Encoding>>() {
        @Override
        protected Map<String, Encoding> initialValue() {
            return new HashMap<String, Encoding>();
        }
    };
}
