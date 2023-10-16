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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities common to testing
 */
public final class TestUtil  {

  private static final String userDir = System.getProperty("user.dir");

  /**
   * TestNG group constants
   */
  public static final String GENERATE_JAVA_FILES = "generate_java_files";
  public static final String CHECK_CPP_FILES = "check_cpp_files";
  public static final String CHECK_CPP_HISTORICAL_FILES = "check_cpp_historical_files";

  /**
   * The full target Path for Java serialized sketches to be tested by other languages.
   */
  public static final Path javaPath = createPath("target/java_generated_files");

  /**
   * The full target Path for C++ serialized sketches to be tested by Java.
   */
  public static final Path cppPath = createPath("target/cpp_generated_files");

  private static Path createPath(final String projectLocalDir) {
    try {
      return Files.createDirectories(Paths.get(userDir, projectLocalDir));
    } catch (IOException e) { throw new SketchesArgumentException(e.getCause().toString()); }
  }

}
