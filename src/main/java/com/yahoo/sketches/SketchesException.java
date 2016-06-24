package com.yahoo.sketches;

/**
 * Exception class for the library
 */
public class SketchesException extends RuntimeException {

    public SketchesException() {
    }

    public SketchesException(String message) {
        super(message);
    }

    public SketchesException(String message, Throwable cause) {
        super(message, cause);
    }

    public SketchesException(Throwable cause) {
        super(cause);
    }

    public SketchesException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
