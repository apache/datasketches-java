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
import java.util.Objects;

/**
 * Utilities common to testing
 */
public final class TestUtil  {

  /**
   * TestNG group constants
   */
  public static final String GENERATE_JAVA_FILES = "generate_java_files";
  public static final String CHECK_CPP_FILES = "check_cpp_files";
  public static final String CHECK_GO_FILES = "check_go_files";
  public static final String CHECK_RUST_FILES = "check_rust_files";
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
   * The project relative Path for Rust serialized sketches to be tested by Java.
   */
  public static final Path rustPath = Path.of(".", "serialization_test_data", "rust_generated_files");

  /**
   * The project relative Path for /src/test/resources
   */
  public static final Path resPath = Path.of(".","src","test","resources");

  public enum Existence { MUST_EXIST, WARNING }

  /**
   * Gets all the bytes of a file as a byte array.
   * If the file is missing, this either throws an exception or writes a warning message to the console
   * based on the state of the optional {@link #Existence Existence}.
   * @param basePath the base directory path where the file is located
   * @param fileName the simple file name of the file
   * @param option an optional parameter. If option == Existence.MUST_EXIST and the file does not exist an exception will be thrown.
   * If option == Existence.WARNING, or not given, and the file does not exist, it writes a warning message
   * to {@link System.err.out System.err.out}.
   * If option has more than one argument an exception will be thrown.
   * @return a byte array. It may be empty.
   * @throws RuntimeException for IO errors, or if resolved path is not a file or not readable or optionally not found.
   */
  public static byte[] getFileBytes(final Path basePath, final String fileName,  Existence... option) {
    Objects.requireNonNull(basePath, "input parameter 'Path basePath' cannot be null.");
    Objects.requireNonNull(fileName, "input parameter 'String fileName' cannot be null.");
    if (option.length > 1) { throw new IllegalArgumentException("Existence option has a maximum of one argument"); }
    Existence status = (option.length == 1) ? option[0] : Existence.WARNING;

    Path path = Path.of(basePath.toString(), fileName);
    Path absPath = path.toAbsolutePath(); //for error output
    if (Files.notExists(path)) {
      if (status == Existence.MUST_EXIST) {
        throw new RuntimeException("File disappeared or not found: " + absPath);
      } else {
        System.err.println("WARNING: File disappeared or not found: " + absPath);
        return new byte[0];
      }
    }
    if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
      throw new RuntimeException("Path is not a regular file or not readable: " + absPath);
    }
    try {
      byte[] bytes = Files.readAllBytes(path);
      return bytes;
    } catch (IOException e) {
        throw new RuntimeException("System IO Error reading file: " + absPath + " " + e);
    }
  }

  /**
   * Puts all the bytes of the given byte array to a file with the given fileName.
   * This assumes that the base directory path is {@link #javaPath javaPath}.
   * @param fileName the name of the target file
   * @param bytes the given byte array
   */
  public static void putBytesToJavaPathFile(final String fileName, final byte[] bytes) {
    putFileBytes(javaPath, fileName, bytes);
  }

  /**
   * Puts all the bytes of the given byte array to a basePath file with the given fileName.
   * If the file exists it will be overwritten.
   * @param basePath the directory path for the given fileName
   * @param fileName the name of the target file
   * @param bytes the given byte array
   * @throws RuntimeException for IO errors,
   */
  public static void putFileBytes(final Path basePath, final String fileName, final byte[] bytes) {
    Objects.requireNonNull(basePath, "input parameter 'Path basePath' cannot be null.");
    Objects.requireNonNull(fileName, "input parameter 'String fileName' cannot be null.");
    Objects.requireNonNull(bytes, "input parameter 'byte[] bytes' cannot be null.");
    Path filePath = null;
    try {
      Files.createDirectories(basePath); //create the directory if it doesn't exist.
      filePath = basePath.resolve(fileName);
      Files.write(filePath, bytes);
    } catch (IOException e) {
      throw new RuntimeException("System IO Error writing file: " + filePath.toString() + " " + e);
    }
  }
}
