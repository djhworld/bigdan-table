package io.github.djhworld.model;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.reflect.TypeParameter;

import java.time.Clock;
import java.util.Iterator;

import static io.github.djhworld.model.RowMutation.Action.*;
import static java.time.LocalDateTime.now;
import static java.time.ZoneOffset.UTC;

public class RowMutation implements Comparable<RowMutation> {
    public static final String TOMBSTONE = "Delete![-TOMBSTONE-]Delete!";
    private static final String SEPARATOR = "|||";
    private static final Joiner JOINER = Joiner.on(SEPARATOR);
    private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
    private static final Clock CLOCK = Clock.systemUTC();
    public final Action action;
    public final long timestamp;
    public final String rowKey;
    public final String columnKey;
    public final String value;

    private RowMutation(Action action, String rowKey, String columnKey, String value) {
        this(action, rowKey, columnKey, value, CLOCK.millis());
    }

    private RowMutation(Action action, String rowKey, String columnKey, String value, long timestamp) {
        this.action = action;
        this.timestamp = timestamp;
        this.rowKey = rowKey;
        this.columnKey = columnKey;
        this.value = value;
    }


    public int size() {
        return rowKey.length() + columnKey.length() + value.length();
    }

    public byte[] serialise() {
        return JOINER.join(
                action,
                rowKey,
                columnKey,
                value,
                timestamp
        ).getBytes();
    }

    public static RowMutation newAddMutation(String rowKey, String columnKey, String value) {
        return new RowMutation(ADD, rowKey, columnKey, value);
    }

    public static RowMutation newAddMutation(String rowKey, String columnKey, String value, long timestamp) {
        return new RowMutation(ADD, rowKey, columnKey, value, timestamp);
    }

    public static RowMutation newDeleteMutation(String rowKey, String columnKey) {
        return new RowMutation(DEL, rowKey, columnKey, TOMBSTONE, Long.MAX_VALUE);
    }

    public static RowMutation deserialise(String value) {
        Iterator<String> split = SPLITTER.split(value).iterator();

        return new RowMutation(
                valueOf(split.next()),
                split.next(),
                split.next(),
                split.next(),
                Long.valueOf(split.next())
        );
    }

    @Override
    public int compareTo(RowMutation o) {
        if (o.timestamp <= timestamp)
            return -1;
        else
            return 1;
    }

    public enum Action {
        ADD,
        DEL
    }
}
