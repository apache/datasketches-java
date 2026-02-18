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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
  public static final String CHECK_GO_FILES = "check_go_files";
  public static final String CHECK_CPP_HISTORICAL_FILES = "check_cpp_historical_files";

  /**
   * The project relative Path for Java serialized sketches to be tested by other languages.
   */
  public static final Path javaPath = Path.of(".", "serialization_test_data", "java_generated_files");

  /**
   * The project relative Path for C++ serialized sketches to be tested by Java.
   */
  public static final Path cppPath = Path.of(".", "serialization_test_data", "cpp_generated_files");

  /**
   * The project relative Path for Go serialized sketches to be tested by Java.
   */
  public static final Path goPath = Path.of(".", "serialization_test_data", "go_generated_files");
  
  /**
   * The project relative Path for /src/test/resources
   */
  public static final Path resPath = Path.of(".","src","test","resources");


  //Get Resources

  private static final int BUF_SIZE = 1 << 13;

  public static byte[] getFileBytes(Path basePath, String fileName) throws RuntimeException {
    Objects.requireNonNull(basePath, "input parameter 'Path basePath' cannot be null.");
    Objects.requireNonNull(fileName, "input parameter 'String fileName' cannot be null.");
    Path path = Path.of(basePath.toString(), fileName);
    Path absPath = path.toAbsolutePath(); //for debugging
    byte[] bytes = new byte[0]; //or null
    if (Files.notExists(path)) {
      System.err.println("File disappeared or not found: " + absPath);
      return bytes; //or null
    }
    if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
      throw new RuntimeException("Path is not a regular file or not readable: " + absPath);
    }
    try {
        bytes = Files.readAllBytes(path);
        return bytes;
      } catch (IOException e) {
        throw new RuntimeException("System Error reading file: " + absPath + " " + e);
      }
    } 
}
