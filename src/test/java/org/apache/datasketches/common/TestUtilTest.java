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
import static org.apache.datasketches.common.TestUtil.getFileBytes;
import static org.apache.datasketches.common.TestUtil.putFileBytes;
import static org.apache.datasketches.common.TestUtil.resPath;
import static org.apache.datasketches.common.TestUtil.Existence.MUST_EXIST;
import static org.apache.datasketches.common.TestUtil.Existence.WARNING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.testng.annotations.Test;

public class TestUtilTest {

  @Test
  public void testGetFileBytes_Success() {// throws IOException {
    byte[] resultBytes = getFileBytes(resPath, "GettysburgAddress.txt");
    assertNotNull(resultBytes);
    String resultString = new String(resultBytes, UTF_8);
    assertTrue(resultString.startsWith("Abraham Lincoln's Gettysburg Address:"));
  }

  @Test
  public void testGetFileBytes_MissingFile_Warning() {
    byte[] resultBytes = getFileBytes(resPath, "Test_NonExistentFile_OK", WARNING); //WARNING is the default
    assertNotNull(resultBytes);
    assertEquals(resultBytes.length, 0, "Should return empty array for missing file.");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetFileBytes_MissingFile_MustExist() {
    getFileBytes(resPath, "Test_NonExistentFile_OK", MUST_EXIST);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetFileBytes_NotRegular_NotReadable() throws IOException {
    getFileBytes(resPath, "");
  }

  private static final Path testPath = Path.of(".", "target", "testDir");

  @Test
  public void testPutBytesToFile() {
    byte[] gettysBytes = getFileBytes(resPath, "GettysburgAddress.txt");
    putFileBytes(testPath, "GettysburgAddressCopy.txt", gettysBytes);
    byte[] gettysBytes2 = getFileBytes(testPath, "GettysburgAddressCopy.txt");
    assertEquals(gettysBytes, gettysBytes2);
  }

}
