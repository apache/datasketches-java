/*
 * Copyright 2015-16, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE
 * file at the project root for terms.
 */

package com.yahoo.memory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.CharArrayReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.testng.annotations.Test;

/**
 * Note: this class requires the resource file:
 * <i>/memory/src/test/resources/memory_mapped.txt</i>.
 *
 * @author Praveenkumar Venkatesan
 */
public class MemoryMappedFileTest {

  @SuppressWarnings("unused")
  @Test(expectedExceptions = RuntimeException.class)
  public void testMapException() throws Exception {
    File dummy = createFile("dummy.txt", "");
    new MemoryMappedFile(dummy, 0, dummy.length()); //zero length
  }

  @SuppressWarnings("unused")
  @Test
  public void testIllegalArgumentException() throws Exception {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    try {
      new MemoryMappedFile(file, -1, Integer.MAX_VALUE);
      fail("Failed: testIllegalArgumentException: Position was negative.");
    } catch (Exception e) {
      // Expected;
    }

    try {
      new MemoryMappedFile(file, 0, -1);
      fail("Failed: testIllegalArgumentException: Size was negative");
    } catch (Exception e) {
      // Expected;
    }

    try {
      new MemoryMappedFile(file, Long.MAX_VALUE, 2);
      fail("Failed: testIllegalArgumentException: Sum of position + size is negative.");
    } catch (Exception e) {
      // Expected;
    }
  }

  @Test
  public void testMemoryMapAndFree() {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    long memCapacity = file.length();

    try {
      MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
      assertEquals(memCapacity, mmf.getCapacity());
      mmf.freeMemory();
      assertEquals(0L, mmf.getCapacity());
      assertEquals(MemoryMappedFile.pageCount(1, 16), 16); //check pageCounter
    } catch (Exception e) {
      fail("Failed: testMemoryMapAndFree()");
    }
  }

  @Test
  public void testMultipleUnMaps() {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    MemoryMappedFile mmf = null;
    try {
      mmf = new MemoryMappedFile(file, 0, file.length());
    } catch (Exception e) {
      fail("Failed: testMultipleUnMaps()");
    }
    mmf.freeMemory();
    mmf.freeMemory();
  }

  @Test
  public void testReadByAnotherProcess() {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    try {
      MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
      mmf.load();
      char[] cbuf = new char[500];
      mmf.getCharArray(500, cbuf, 0, 500);
      CharArrayReader car = new CharArrayReader(cbuf);

      MemoryRegion mr = new MemoryRegion(mmf, 500, 3000);
      char[] dst = new char[500];
      mr.getCharArray(0, dst, 0, 500);
      CharArrayReader carMr = new CharArrayReader(dst);
      int value = 0;
      while ((value = car.read()) != -1) {
        assertEquals((char) value, (char) carMr.read());
      }
      mmf.freeMemory();
    } catch (Exception e) {
      fail("Failed: testReadByAnotherProcess()");
    }
  }

  @Test
  public void testReadFailAfterFree() {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    try {
      MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
      mmf.freeMemory();
      char[] cbuf = new char[500];
      try {
        mmf.getCharArray(500, cbuf, 0, 500);
      } catch (AssertionError e) {
        // pass
      }
    } catch (Exception e) {
      fail("Failed: testReadFailAfterFree()");
    }
  }

  @Test
  public void testLoad() {
    File file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    try {
      MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
      mmf.load();
      assertTrue(mmf.isLoaded());
      mmf.freeMemory();
    } catch (Exception e) {
      fail("Failed: testLoad()");
    }
  }

  @Test
  public void testForce() throws Exception {
    File org = createFile("force_original.txt", "Corectng spellng mistks");
    long orgBytes = org.length();
    try {
      // extra 5bytes for buffer
      int buf = (int) orgBytes + 5;
      MemoryMappedFile mmf = new MemoryMappedFile(org, 0, buf);
      mmf.load();

      // existing content
      byte[] c = new byte[buf];
      mmf.getByteArray(0, c, 0, c.length);

      // add content
      String cor = "Correcting spelling mistakes";
      byte[] b = cor.getBytes(UTF_8);
      mmf.putByteArray(0, b, 0, b.length);

      mmf.force();
      mmf.freeMemory();

      MemoryMappedFile nmf = new MemoryMappedFile(org, 0, buf);
      nmf.load();

      // existing content
      byte[] n = new byte[buf];
      nmf.getByteArray(0, n, 0, n.length);

      int index = 0;
      boolean corrected = true;

      // make sure that new content is diff
      while (index < buf) {
        if (b[index] != n[index]) {
          corrected = false;
          break;
        }
        index++;
      }

      assertTrue(corrected);

      nmf.freeMemory();
    } catch (Exception e) {
      fail("Failed: testForce()." + e);
    }
  }

  @Test
  public void checkPassThrough() {
    NativeMemory mem = new AllocMemory(1024L);
    mem.freeMemory();
  }

  private static File createFile(String fileName, String text) throws FileNotFoundException {
    File file = new File(fileName);
    file.deleteOnExit();
    PrintWriter writer;
    try {
      writer = new PrintWriter(file, UTF_8.name());
      writer.print(text);
      writer.close();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return file;
  }

}
