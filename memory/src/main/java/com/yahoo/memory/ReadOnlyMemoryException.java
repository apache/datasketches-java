/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

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

