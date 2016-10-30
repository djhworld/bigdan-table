package io.github.djhworld.model;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.Iterator;

public class RowMutation {
    private static final String SEPARATOR = "|||";
    private static final Joiner JOINER = Joiner.on(SEPARATOR);
    private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
    public final Action action;
    public final String rowKey;
    public final String columnKey;
    public final String value;

    private RowMutation(Action action, String rowKey, String columnKey, String value) {
        this.action = action;
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
                value
        ).getBytes();
    }

    public static RowMutation newAddMutation(String rowKey, String columnKey, String value) {
        return new RowMutation(Action.ADD, rowKey, columnKey, value);
    }

    public static RowMutation newDeleteMutation(String rowKey, String columnKey) {
        return new RowMutation(Action.DEL, rowKey, columnKey, "");
    }

    public static RowMutation deserialise(String value) {
        Iterator<String> split = SPLITTER.split(value).iterator();

        return new RowMutation(
                Action.valueOf(split.next()),
                split.next(),
                split.next(),
                split.next()
        );
    }


    public enum Action {
        ADD,
        DEL
    }
}
