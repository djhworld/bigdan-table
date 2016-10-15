package io.github.djhworld.exception;

public class TabletException extends RuntimeException {
    public TabletException(String message) {
        super(message);
    }

    public TabletException(String message, Throwable cause) {
        super(message, cause);
    }
}
