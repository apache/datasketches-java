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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.common.UtilityIO.getCppPath;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.getJavaPath;
import static org.apache.datasketches.common.UtilityIO.getProjectRoot;
import static org.apache.datasketches.common.UtilityIO.putBytesToFile;
import static org.apache.datasketches.common.UtilityIO.Existence.MUST_EXIST;
import static org.apache.datasketches.common.UtilityIO.Existence.WARNING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UtilityIOTest {

  @Test
  public void checkDirCreation() {
    assertNotNull(getJavaPath());
    assertNotNull(getCppPath());
  }

  @Test
  public void testGetFileBytes_Success() {// throws IOException {
    byte[] resultBytes = UtilityIO.getTestResourceBytes("GettysburgAddress.txt");
    assertNotNull(resultBytes);
    String resultString = new String(resultBytes, UTF_8);
    assertTrue(resultString.startsWith("Abraham Lincoln's Gettysburg Address:"));
  }

  @Test
  public void testGetFileBytes_MissingFile_Warning() {
    byte[] resultBytes = getFileBytes(getProjectRoot(), "Test_NonExistentFile_ThisIsOK", WARNING);
    assertNotNull(resultBytes);
    assertEquals(resultBytes.length, 0, "Should return empty array for missing file.");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetFileBytes_MissingFile_MustExist() {
    getFileBytes(getProjectRoot(), "Test_NonExistentFile_ThisIsOK", MUST_EXIST);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetFileBytes_NotRegular_NotReadable() throws IOException {
    getFileBytes(getProjectRoot(), "");
  }

  private Path tempDir;

  @BeforeMethod
  public void setUp() throws IOException {
    // Creates a unique temporary directory in the OS temp location
    tempDir = Files.createTempDirectory("testDir_");
  }

  @Test
  public void testPutBytesToFile() {
    byte[] gettysBytes = UtilityIO.getTestResourceBytes("GettysburgAddress.txt");
    putBytesToFile(tempDir, "GettysburgAddressCopy.txt", gettysBytes);
    byte[] gettysBytes2 = getFileBytes(tempDir, "GettysburgAddressCopy.txt");
    assertEquals(gettysBytes, gettysBytes2);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    // Recursively deletes the temp directory and all enclosed files after the test runs
    if (tempDir != null && Files.exists(tempDir)) {
      try (var stream = Files.walk(tempDir)) {
        stream.sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
      }
    }
  }

}
