package io.github.djhworld.model;

import com.google.common.base.Splitter;

import java.util.Iterator;

import static com.google.common.base.Joiner.on;

public class RowMutation {
    public static final String SEPARATOR = "|||";
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

    public byte[] serialise() {
        return on(SEPARATOR).join(
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
        Iterator<String> split = Splitter.on(SEPARATOR)
                .split(value).iterator();

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
