/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.io.CharArrayReader;
import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Praveenkumar Venkatesan
 */
public class MemoryMappedFileTest {

    private File file;

    @BeforeClass
    public void setUp() throws Exception {
        file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    }

    @SuppressWarnings("unused")
    @Test
    public void testMapException() {
        File dummy = new File(getClass().getClassLoader().getResource("dummy.txt").getFile());

        try {
            new MemoryMappedFile(dummy, 0, dummy.length());
        } catch (Exception e) {
            assertTrue(true);
            return;
        }

        assertFalse(true);
    }

    @SuppressWarnings("unused")
    @Test
    public void testIllegalArgumentException() throws Exception {
        try {
            new MemoryMappedFile(file, -1, Integer.MAX_VALUE);
        } catch (Exception e) {
            assertTrue(true);
            return;
        }

        try {
            new MemoryMappedFile(file, 0, -1);
        } catch (Exception e) {
            assertTrue(true);
            return;
        }

        try {
            new MemoryMappedFile(file, 1, -2);
        } catch (Exception e) {
            assertTrue(true);
            return;
        }
    }

    @Test
    public void testMemoryMapAndFree() {
        long memCapacity = file.length();

        try {
            MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
            assertEquals(memCapacity, mmf.getCapacity());
            mmf.freeMemory();
            assertEquals(0L, mmf.getCapacity());
        } catch (Exception e) {
            assertFalse(true);
        }
    }

    @Test
    public void testMultipleUnMaps() {
        try {
            MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
            mmf.freeMemory();
            mmf.freeMemory();
        } catch (Exception e) {
            assertTrue(true);
            return;
        }
        assertFalse(true);

    }

    @Test
    public void testReadByAnotherProcess() {
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
                assertEquals((char)value, (char)carMr.read());
            }
            mmf.freeMemory();
        } catch (Exception e) {
            assertFalse(true);
        }
    }

    @Test
    public void testReadFailAfterFree() {
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
            assertFalse(true);
        }
    }

    @Test
    public void testLoad() {
        try {
            MemoryMappedFile mmf = new MemoryMappedFile(file, 0, file.length());
            mmf.load();
            assertTrue(mmf.isLoaded());
            mmf.freeMemory();
        } catch (Exception e) {
            assertFalse(true);
        }
    }

    @Test
    public void testForce() {
        File org = new File(getClass().getClassLoader().getResource("force_original.txt").getFile());
        long orgBytes = org.length();

        try {
            // extra 5bytes for buffer
            int buf = (int)orgBytes + 5;

            MemoryMappedFile mmf = new MemoryMappedFile(org, 0, buf);
            mmf.load();

            // existing content
            byte[] c = new byte[buf];
            mmf.getByteArray(0, c, 0, c.length);

            // add content
            String cor = "Correcting spelling mistakes";
            byte[] b = cor.getBytes();
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
            assertFalse(true);
        }
    }
}
