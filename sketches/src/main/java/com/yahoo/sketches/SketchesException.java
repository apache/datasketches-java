/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * Exception class for the library
 *
 * @author Lee Rhodes
 */
public class SketchesException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  //other constructors to be added as needed.

  /**
   * Constructs a new runtime exception with the specified detail message. The cause is not
   * initialized, and may subsequently be initialized by a call to
   * Throwable.initCause(java.lang.Throwable).
   *
   * @param message the detail message. The detail message is saved for later retrieval by the
   * Throwable.getMessage() method.
   */
  public SketchesException(final String message) {
    super(message);
  }

  /**
   * Constructs a new runtime exception with the specified detail message and cause.
   *
   * <p>Note that the detail message associated with cause is not automatically incorporated
   * in this runtime exception's detail message.</p>
   *
   * @param message the detail message (which is saved for later retrieval by the
   * Throwable.getMessage() method).
   * @param cause the cause (which is saved for later retrieval by the Throwable.getCause()
   * method). (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
   */
  public SketchesException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
