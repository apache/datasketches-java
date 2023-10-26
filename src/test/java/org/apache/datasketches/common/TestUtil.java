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
  public static final String CHECK_CPP_HISTORICAL_FILES = "check_cpp_historical_files";

  /**
   * The full target Path for Java serialized sketches to be tested by other languages.
   */
  public static final Path javaPath = createPath("target2/java_generated_files");

  /**
   * The full target Path for C++ serialized sketches to be tested by Java.
   */
  public static final Path cppPath = createPath("target2/cpp_generated_files");

  private static Path createPath(final String projectLocalDir) {
    try {
      return Files.createDirectories(Paths.get(userDir, projectLocalDir));
    } catch (IOException e) { throw new SketchesArgumentException(e.getCause().toString()); }
  }

  //Get Resources

  private static final int BUF_SIZE = 1 << 13;

  /**
   * Gets the file defined by the given resource file's shortFileName.
   * @param shortFileName the last name in the pathname's name sequence.
   * @return the file defined by the given resource file's shortFileName.
   */
  public static File getResourceFile(final String shortFileName) {
    Objects.requireNonNull(shortFileName, "input parameter 'String shortFileName' cannot be null.");
    final String slashName = (shortFileName.charAt(0) == '/') ? shortFileName : '/' + shortFileName;
    final URL url = Util.class.getResource(slashName);
    Objects.requireNonNull(url, "resource " + slashName + " returns null URL.");
    File file;
    file = createTempFile(slashName);
    if (url.getProtocol().equals("jar")) { //definitely a jar
      try (final InputStream input = Util.class.getResourceAsStream(slashName);
        final OutputStream out = new FileOutputStream(file)) {
        Objects.requireNonNull(input, "InputStream  is null.");
        int numRead = 0;
        final byte[] buf = new byte[1024];
        while ((numRead = input.read(buf)) != -1) { out.write(buf, 0, numRead); }
      } catch (final IOException e ) { throw new RuntimeException(e); }
    } else { //protocol says resource is not a jar, must be a file
      file = new File(getResourcePath(url));
    }
    if (!file.setReadable(false, true)) {
      throw new IllegalStateException("Failed to set owner only 'Readable' on file");
    }
    if (!file.setWritable(false, false)) {
      throw new IllegalStateException("Failed to set everyone 'Not Writable' on file");
    }
    return file;
  }

  /**
   * Returns a byte array of the contents of the file defined by the given resource file's shortFileName.
   * @param shortFileName the last name in the pathname's name sequence.
   * @return a byte array of the contents of the file defined by the given resource file's shortFileName.
   * @throws IllegalArgumentException if resource cannot be read.
   */
  public static byte[] getResourceBytes(final String shortFileName) {
    Objects.requireNonNull(shortFileName, "input parameter 'String shortFileName' cannot be null.");
    final String slashName = (shortFileName.charAt(0) == '/') ? shortFileName : '/' + shortFileName;
    final URL url = Util.class.getResource(slashName);
    Objects.requireNonNull(url, "resource " + slashName + " returns null URL.");
    final byte[] out;
    if (url.getProtocol().equals("jar")) { //definitely a jar
      try (final InputStream input = Util.class.getResourceAsStream(slashName)) {
        out = readAllBytesFromInputStream(input);
      } catch (final IOException e) { throw new RuntimeException(e); }
    } else { //protocol says resource is not a jar, must be a file
      try {
        out = Files.readAllBytes(Paths.get(getResourcePath(url)));
      } catch (final IOException e) { throw new RuntimeException(e); }
    }
    return out;
  }

  /**
   * Note: This is only needed in Java 8 as it is part of Java 9+.
   * Read all bytes from the given <i>InputStream</i>.
   * This is limited to streams that are no longer than the maximum allocatable byte array determined by the VM.
   * This may be a little smaller than <i>Integer.MAX_VALUE</i>.
   * @param in the Input Stream
   * @return byte array
   */
  public static byte[] readAllBytesFromInputStream(final InputStream in) {
    return readBytesFromInputStream(Integer.MAX_VALUE, in);
  }

  /**
   * Note: This is only needed in Java 8 as is part of Java 9+.
   * Read <i>numBytesToRead</i> bytes from an input stream into a single byte array.
   * This is limited to streams that are no longer than the maximum allocatable byte array determined by the VM.
   * This may be a little smaller than <i>Integer.MAX_VALUE</i>.
   * @param numBytesToRead number of bytes to read
   * @param in the InputStream
   * @return the filled byte array from the input stream
   * @throws IllegalArgumentException if array size grows larger than what can be safely allocated by some VMs.

   */
  public static byte[] readBytesFromInputStream(final int numBytesToRead, final InputStream in) {
    if (numBytesToRead < 0) { throw new IllegalArgumentException("numBytesToRead must be positive or zero."); }

    List<byte[]> buffers = null;
    byte[] result = null;
    int totalBytesRead = 0;
    int remaining = numBytesToRead;
    int chunkCnt;
    do {
        final byte[] partialBuffer = new byte[Math.min(remaining, BUF_SIZE)];
        int numRead = 0;

        try {
          // reads input stream in chunks of partial buffers, stops at EOF or when remaining is zero.
          while ((chunkCnt =
                in.read(partialBuffer, numRead, Math.min(partialBuffer.length - numRead, remaining))) > 0) {
              numRead += chunkCnt;
              remaining -= chunkCnt;
          }
        } catch (final IOException e) { throw new RuntimeException(e); }

        if (numRead > 0) {
            if (Integer.MAX_VALUE - Long.BYTES - totalBytesRead < numRead) {
              throw new IllegalArgumentException(
                  "Input stream is larger than what can be safely allocated as a byte[] in some VMs."); }
            totalBytesRead += numRead;
            if (result == null) {
                result = partialBuffer;
            } else {
                if (buffers == null) {
                    buffers = new ArrayList<>();
                    buffers.add(result);
                }
                buffers.add(partialBuffer);
            }
        }
    } while (chunkCnt >= 0 && remaining > 0);

    final byte[] out;
    if (buffers == null) {
        if (result == null) {
          out = new byte[0];
        } else {
          out = result.length == totalBytesRead ? result : Arrays.copyOf(result, totalBytesRead);
        }
        return out;
    }

    result = new byte[totalBytesRead];
    int offset = 0;
    remaining = totalBytesRead;
    for (byte[] b : buffers) {
        final int count = Math.min(b.length, remaining);
        System.arraycopy(b, 0, result, offset, count);
        offset += count;
        remaining -= count;
    }
    return result;
  }

  private static String getResourcePath(final URL url) { //must not be null
    try {
      final URI uri = url.toURI();
      //decodes any special characters
      final String path = uri.isAbsolute() ? Paths.get(uri).toAbsolutePath().toString() : uri.getPath();
      return path;
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException("Cannot find resource: " + url.toString() + Util.LS + e);
    }
  }

  /**
   * Create an empty temporary file.
   * On a Mac these files are stored at the system variable $TMPDIR.  They should be cleared on a reboot.
   * @param shortFileName the name before prefixes and suffixes are added here and by the OS.
   * The final extension will be the current extension. The prefix "temp_" is added here.
   * @return a temp file,which will be eventually deleted by the OS
   */
  private static File createTempFile(final String shortFileName) {
    //remove any leading slash
    final String resName = (shortFileName.charAt(0) == '/') ? shortFileName.substring(1) : shortFileName;
    final String suffix;
    final String name;
    final int  lastIdx = resName.length() - 1;
    final int lastIdxOfDot = resName.lastIndexOf('.');
    if (lastIdxOfDot == -1) {
      suffix = ".tmp";
      name = resName;
    } else if (lastIdxOfDot == lastIdx) {
      suffix = ".tmp";
      name = resName.substring(0, lastIdxOfDot);
    } else { //has a real suffix
      suffix = resName.substring(lastIdxOfDot);
      name = resName.substring(0, lastIdxOfDot);
    }
    final File file;
    try {
      file = File.createTempFile("temp_" + name, suffix);
      if (!file.setReadable(false, true)) {
        throw new IllegalStateException("Failed to set only owner 'Readable' on file");
      }
      if (!file.setWritable(false, true)) {
        throw new IllegalStateException("Failed to set only owner 'Writable' on file");
      }

    } catch (final IOException e) { throw new RuntimeException(e); }
    return file;
  }

}
