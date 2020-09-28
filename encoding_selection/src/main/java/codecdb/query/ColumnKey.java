package codecdb.query;


import java.util.Arrays;
import java.util.stream.Collectors;

final class ColumnKey {
    private String[] keys;

    ColumnKey(String[] keys) {
        this.keys = keys;
    }

    @Override
    public int hashCode() {
        return Arrays.stream(keys).collect(Collectors.joining(":")).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ColumnKey) {
            return Arrays.equals(keys, ((ColumnKey) other).keys);
        }
        return super.equals(other);
    }
}