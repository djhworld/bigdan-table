package io.github.djhworld.exception;

public class SSTableException extends RuntimeException {
    public SSTableException(String message) {
        super(message);
    }

    public SSTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
