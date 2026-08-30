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
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Utilities common to testing
 */
public final class UtilityIO  {
  private static final String LS = System.getProperty("line.separator");
  private static final Class<?> clazz = UtilityIO.class;
  private static final ClassLoader CL = clazz.getClassLoader();
  private static final String TEST_DATA_ROOT_PROP = "test.data.root";
  private static final String PROJECT_ROOT_PROP = "project.root";

  /**
   * TestNG group constants
   */
  public static final String GENERATE_JAVA_FILES = "generate_java_files";
  public static final String CHECK_JAVA_FILES = "check_java_files";
  public static final String CHECK_CPP_FILES = "check_cpp_files";
  public static final String CHECK_GO_FILES = "check_go_files";
  public static final String CHECK_RUST_FILES = "check_rust_files";
  public static final String CHECK_CPP_HISTORICAL_FILES = "check_cpp_historical_files";

  public enum Existence { MUST_EXIST, WARNING }

  /**
   * For passing variables to a multi-language test method leveraging TestNG Groups.
   *
   * <p>Usage in a method would look like this:</p>
   * {@snippet :
   * public void testMethod(GpLanguage lang) {
   *   String suffix = lang.sfx;
   *   Path path = lang.pth;
   *   ...
   * }
   * }
   */
  public enum GroupLanguage {
    JAVA("_java", getJavaPath()),
    CPP("_cpp", getCppPath()),
    GO("_go", getGoPath()),
    RUST("_rust", getRustPath());


    public final String sfx;
    public final Path pth;

    GroupLanguage(String suffix, Path path) {
        this.sfx = suffix;
        this.pth = path;
    }
  }

  /**
   * The project relative Path for Java serialized sketches to be tested by other languages.
   */
  public static Path getJavaPath() {
    return getTestDataRoot().resolve("serialization_test_data", "java_generated_files");
  }

  /**
   * The project relative Path for C++ serialized sketches to be tested by Java.
   */
  public static Path getCppPath() {
    return getTestDataRoot().resolve("serialization_test_data", "cpp_generated_files");
  }

  /**
   * The project relative Path for Go serialized sketches to be tested by Java.
   */
  public static Path getGoPath() {
    return getTestDataRoot().resolve("serialization_test_data", "go_generated_files");
  }

  /**
   * The project relative Path for Rust serialized sketches to be tested by Java.
   */
  public static Path getRustPath() {
    return getTestDataRoot().resolve("serialization_test_data", "rust_generated_files");
  }

  /**
   * Gets all the bytes of a file as a byte array.
   * If the file is missing this issues a Warning message to the console.
   * @param basePath the directory where <i>fileName</i> is located.
   * @param fileName the simple file name of the file.
   * @return a byte array. It will be empty if file not found.
   * @throws RuntimeException for IO errors.
   */
  public static byte[] getFileBytes(final Path basePath, final String fileName) {
    return getFileBytes(basePath, fileName, Existence.WARNING);
  }

  /**
   * Gets all the bytes of a file as a byte array.
   * If the file is missing this issues a Warning message to the console or throws a RuntimeException
   * based on the state of Existence.
   * @param basePath the directory where <i>fileName</i> is located.
   * @param fileName the simple file name of the file.
   * @return a byte array. It will be empty if file not found and Existence is <i>WARNING</i>
   * @throws RuntimeException for IO errors,
   */
  public static byte[] getFileBytes(final Path basePath, final String fileName,  final Existence existence) {
    Objects.requireNonNull(basePath, "input parameter 'Path basePath' cannot be null.");
    Objects.requireNonNull(fileName, "input parameter 'String fileName' cannot be null.");
    Objects.requireNonNull(existence, "input parameter 'Existence existence' cannot be null.");

    Path path = basePath.resolve(fileName).normalize();
    String pathDisplay;
    try {
      pathDisplay = path.toAbsolutePath().toString();
    } catch (SecurityException e) {
      pathDisplay = path.toString();
    }

    try {
      return Files.readAllBytes(path);
    } catch (NoSuchFileException e) {
      if (existence == Existence.MUST_EXIST) {
          throw new RuntimeException("File not found: " + pathDisplay, e);
      }
      System.err.println("WARNING: File not found: " + pathDisplay);
      return new byte[0];
    } catch (AccessDeniedException e) {
      throw new RuntimeException("Permission denied reading file: " + pathDisplay, e);
    } catch (IOException e) {
      throw new RuntimeException("IO Error reading file: " + pathDisplay, e);
    }
  }

  /**
   * This gets a byte[] from a resource file in /src/test/resources whether called on an install or a jar.
   * These files must exist.
   * @param fileName the desired filename
   * @return the bytes of the file as a byte[]
   * @throws IllegalArgumentException if file not found.
   */
  public static byte[] getTestResourceBytes(final String fileName) {
    Objects.requireNonNull(fileName, "fileName cannot be null");
    // Strip leading slash if present, as ClassLoader doesn't accept leading slashes
    String resourcePath = fileName.startsWith("/") ? fileName.substring(1) : fileName;
    try (InputStream is = CL.getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalArgumentException("Resource not found on classpath: " + resourcePath);
    }
      return is.readAllBytes();
    } catch (final IOException e){
      throw new RuntimeException("Error reading resource: " + resourcePath, e);
    }
  }

  /**
   * Puts all the bytes of the given byte array to a file with the given fileName.
   * This assumes that the base directory path is {@link #javaPath javaPath}.
   * @param fileName the name of the target file
   * @param bytes the given byte array
   */
  public static void putBytesToJavaPath(final String fileName, final byte[] bytes) {
    putBytesToFile(getJavaPath(), fileName, bytes);
  }

  /**
   * Puts all the bytes of the given byte array to a basePath file with the given fileName.
   * If the file exists it will be overwritten. Called from putBytesToJavaPath() and TestUtilTest.
   *
   * <p>Used by putBytesToJavaPath(...)
   * @param basePath the directory path for the given fileName
   * @param fileName the name of the target file
   * @param bytes the given byte array
   * @throws RuntimeException for IO errors,
   */
  static void putBytesToFile(final Path basePath, final String fileName, final byte[] bytes) {
    Objects.requireNonNull(basePath, "input parameter 'Path basePath' cannot be null.");
    Objects.requireNonNull(fileName, "input parameter 'String fileName' cannot be null.");
    Objects.requireNonNull(bytes, "input parameter 'byte[] bytes' cannot be null.");
    Path filePath = null;
    try {
      Files.createDirectories(basePath); //create the directory if it doesn't exist.
      filePath = basePath.resolve(fileName);
      Files.write(filePath, bytes);
    } catch (IOException e) {
      final String filePathDesc = String.valueOf(filePath);
      throw new RuntimeException("System IO Error writing file: " + filePathDesc + " " + e);
    }
  }

  /**
   * Resolves the target directory for test data generation/reading.
   * The default is the project root, but it can be overridden like this:
   *
   * <p><i>mvn clean test -Pcheck-cpp-files -Dtest.data.root="/tmp/custom_path</i></p>
   *
   * <p>Calling the binary test jar: see src/test/resources/testng.xml</p>
   *
   * Used by getJavaPath(), getCppPath(), getGoPath(), getRustPath()
   *
   * @return Path to the test.data.root directory.
   * @throws IllegalStateException if running from a JAR without -Dtest.data.root defined.
   */
  private static Path getTestDataRoot() {
    // Explicit user override or Maven-injected property
    String sysProp = System.getProperty(TEST_DATA_ROOT_PROP);
    if (sysProp != null && !sysProp.isBlank()) {
      return Path.of(sysProp).toAbsolutePath().normalize();
    }

    checkExecutionFromJar(TEST_DATA_ROOT_PROP);

    // Fallback for local source checkout / IDE runs (walk up to top-level .mvn)
    return getProjectRoot();
  }

  /**
   * Resolves the project root
   * The default is the project root, but it can be overridden like this:
   *
   * <p><i>mvn clean test -Dproject.root="/tmp/custom_path</i></p>
   *
   * <p>Calling the binary test jar: see src/test/resources/testng.xml</p>
   *
   * Used by getTestDataRoot(), projectRoot
   *
   * @return Path to the project.root directory.
   * @throws IllegalStateException if running from a JAR without -Dproject.root defined.
   */
  public static Path getProjectRoot() {
    // Explicit user override or Maven-injected property
    String sysProp = System.getProperty(PROJECT_ROOT_PROP);
    if (sysProp != null && !sysProp.isBlank()) {
      return Path.of(sysProp).toAbsolutePath().normalize();
    }

    // Strict JAR Execution Rule: Must explicitly provide -Dproject.root = String path
    checkExecutionFromJar(PROJECT_ROOT_PROP);

    // Fallback for local source checkout / IDE runs (walk up to top-level .mvn)
    Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    while (current != null) {
      if (Files.exists(current.resolve(".mvn"))) {
        return current.normalize();
      }
      current = current.getParent();
    }
    throw new IllegalStateException("Could not find project root containing '.mvn' directory");
  }

  private static void checkExecutionFromJar(final String root) {
    var url = clazz.getResource(clazz.getSimpleName() + ".class");
    if (url != null && "jar".equals(url.getProtocol())) {
      throw new IllegalStateException(
        "Executing from a packaged JAR file requires an explicit target directory." + LS +
        "Please specify the output directory using: -D" + root + "=/path/to/directory"
      );
    }
  }

}
