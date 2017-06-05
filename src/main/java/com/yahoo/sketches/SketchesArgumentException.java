/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * Illegal Arguments Exception class for the library
 *
 * @author Lee Rhodes
 */
public class SketchesArgumentException extends SketchesException {
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
  public SketchesArgumentException(final String message) {
    super(message);
  }
}
