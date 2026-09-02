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

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class A_BeforeSuite {

  /**
   * For counting cross-language test cases.
   */
  private static final AtomicInteger writeCount = new AtomicInteger(0);
  private static final AtomicInteger readCount = new AtomicInteger(0);
  private static final AtomicInteger warnings = new AtomicInteger(0);

  public static void incWriteCount() {
    writeCount.incrementAndGet();
  }

  public static void incReadCount() {
    readCount.incrementAndGet();
  }

  public static void incWarnings() {
    warnings.incrementAndGet();
  }

  public static int getWriteCount() {
    return writeCount.get();
  }

  public static int getReadCount() {
    return readCount.get();
  }

  public static int getWarnings() {
    return warnings.get();
  }

  public static void resetCounts() {
    writeCount.set(0);
    readCount.set(0);
    warnings.set(0);
  }

  @BeforeSuite(alwaysRun = true)
  public void printTestEnvironment() {
    System.out.println("====================================================");
    System.out.println("TEST JDK: " + System.getProperty("java.version"));
    System.out.println("TEST JDK HOME: " + System.getProperty("java.home"));
    System.out.println("=====================================================");
    resetCounts();
  }

  @AfterSuite(alwaysRun = true)
  public void printCountResult() {
    System.out.println("====================================================");
    System.out.println("Total Files Written: " + getWriteCount());
    System.out.println("Total Files Read   : " + getReadCount());
    System.out.println("Total File Warnings: " + getWarnings());
    System.out.println("====================================================");
  }
}
