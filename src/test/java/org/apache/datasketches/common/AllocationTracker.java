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

package org.apache.datasketches.common;

import java.lang.management.ManagementFactory;

/**
 * Test utility that measures how many bytes the current thread allocates while running a task.
 * Used to check that deserializing a small, corrupt image does not cause a large allocation.
 */
public final class AllocationTracker {
  private static final com.sun.management.ThreadMXBean TMX =
      (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

  private AllocationTracker() { }

  /**
   * Runs the given task and returns the number of bytes the current thread allocated while running it.
   * Any exception thrown by the task is ignored.
   * @param task the task to run
   * @return the number of bytes allocated
   */
  public static long allocatedBytes(final Runnable task) {
    final long tid = Thread.currentThread().threadId();
    final long before = TMX.getThreadAllocatedBytes(tid);
    try { task.run(); } catch (final RuntimeException e) { /* ignored */ }
    return TMX.getThreadAllocatedBytes(tid) - before;
  }
}
