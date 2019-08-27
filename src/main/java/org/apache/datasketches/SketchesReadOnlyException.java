/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches;

/**
 * Write operation attempted on a read-only class.
 *
 * @author Lee Rhodes
 */
public class SketchesReadOnlyException extends SketchesException {
  private static final long serialVersionUID = 1L;

  //other constructors to be added as needed.

  /**
   * Constructs a new runtime exception with the message:
   * "Write operation attempted on a read-only class."
   *
   * <p>The cause is not initialized, and may subsequently be initialized by a call to
   * Throwable.initCause(java.lang.Throwable).
   */
  public SketchesReadOnlyException() {
    super("Write operation attempted on a read-only class.");
  }

  /**
   * Constructs a new runtime exception with the specified detail message. The cause is not
   * initialized, and may subsequently be initialized by a call to
   * Throwable.initCause(java.lang.Throwable).
   *
   * @param message the detail message. The detail message is saved for later retrieval by the
   * Throwable.getMessage() method.
   */
  public SketchesReadOnlyException(final String message) {
    super(message);
  }
}
