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

import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class SketchesExceptionTest {

  @Test(expectedExceptions = SketchesException.class)
  public void checkSketchesException() {
    throw new SketchesException("This is a test.");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSketchesArgumentException() {
    throw new SketchesArgumentException("This is a test.");
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkSketchesStateException() {
    throw new SketchesStateException("This is a test.");
  }

  @Test
  public void checkSketchesExceptionWithThrowable() {
    try {
      throw new SketchesException("First Exception.");
    } catch (final SketchesException se) {
      try {
        throw new SketchesException("Second Exception. ", se);
      } catch (final SketchesException se2) {
        //success
      }
    }
  }

}
