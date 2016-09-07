package com.yahoo.memory;

/**
 * The exception thrown by the write methods of the read-only classes.
 * 
 * @author Praveenkumar Venkatesan
 */
public class ReadOnlyMemoryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ReadOnlyMemoryException() { }
}

