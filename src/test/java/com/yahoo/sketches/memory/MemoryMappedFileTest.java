/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileNotFoundException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by Praveenkumar Venkatesan on 7/19/16.
 */
public class MemoryMappedFileTest {

    private File file;

    @BeforeClass
    public void setUp() throws Exception {
        file = new File(getClass().getClassLoader().getResource("memory_mapped.txt").getFile());
    }

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

    @Test
    public void testIllegalArgumentException() {

        try {
            new MemoryMappedFile(file, 0, Integer.MAX_VALUE);
        } catch (Exception e) {
            assertTrue(true);
            return;
        }

        assertFalse(true);
    }

    @Test
    public void testMemoryMapAndFree() {
        long memCapacity = file.length();

        MemoryMappedFile mmf = null;
        try {
            mmf = new MemoryMappedFile(file, 0, file.length());
            assertEquals(memCapacity, mmf.getCapacity());
            mmf.freeMemory();
            assertEquals(0L, mmf.getCapacity());
        } catch (Exception e) {
            assertFalse(true);
        }
    }

    @Test
    public void testMultipleUnMaps() {
        MemoryMappedFile mmf = null;
        try {
            mmf = new MemoryMappedFile(file, 0, file.length());
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
        MemoryMappedFile mmf = null;
        try {
            mmf = new MemoryMappedFile(file, 0, file.length());
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
        MemoryMappedFile mmf = null;
        try {
            mmf = new MemoryMappedFile(file, 0, file.length());
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
    public void testLoad() throws Exception {
        MemoryMappedFile mmf = null;
        try {
            mmf = new MemoryMappedFile(file, 0, file.length());
            mmf.load();
            assertTrue(mmf.isLoaded());
        } catch (FileNotFoundException e) {
            assertFalse(true);
        }
    }

}
