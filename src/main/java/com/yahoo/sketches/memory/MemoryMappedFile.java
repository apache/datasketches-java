/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import sun.nio.ch.FileChannelImpl;

/**
 * MemoryMappedFile class extends NativeMemory and is used to memory map files 
 * (including those &gt; 2GB) off heap. It is the responsibility of the calling class to free the 
 * memory.
 *
 * <p>This is a little bit backwards: By rights MemoryMappedFile should be a
 * subclass of AllocMemory, but to keep the specification clear and simple,
 * it's easier to do it the other way around.
 *
 * @author Praveenkumar Venkatesan
 */
public class MemoryMappedFile extends NativeMemory {

    private FileChannel fileChannel = null;
    private RandomAccessFile randomAccessFile = null;

    /**
     * Constructor for memory mapping a file.
     * 
     * <p>Memory maps a file directly in off heap leveraging native map0 method used in 
     * FileChannelImpl.c. The owner will have read write access to that address space.</p>
     * @param file - File to be mapped
     * @param position - memory map starting from this position in the file
     * @param len - memory map at most len bytes starting from {@code position}
     *
     * @throws FileNotFoundException file not found
     */
    public MemoryMappedFile(File file, long position, long len) throws FileNotFoundException {
        super(0L, null, null);

        if (len > file.length()) {
            throw new IllegalArgumentException("Can only map at most the length of a file");
        }

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        super.nativeRawStartAddress_ = map(position, len);
        super.capacityBytes_ = len;
        super.memReq_ = null;
    }
    
    //pass-through
    MemoryMappedFile(long objectBaseOffset, Object memArray, ByteBuffer byteBuf) {
        super(objectBaseOffset, memArray, byteBuf);
    }

    /**
     * Loads content into physical memory.
     * This method makes a best effort to ensure that, when it returns, this buffer's content is 
     * resident in physical memory. Invoking this method may cause some number of page faults 
     * and I/O operations to occur.
     */
    public void load() {
        if (capacityBytes_ == 0) {
            return;
        }

        madvise();

        // Read a byte from each page to bring it into memory.
        int ps = unsafe.pageSize();
        int count = pageCount(ps, capacityBytes_);
        long a = nativeRawStartAddress_;
        for (int i = 0; i < count; i++) {
            unsafe.getByte(a);
            a += ps;
        }
    }

    @Override
    public void freeMemory() {
        if (fileChannel != null) {
            unmap();
            nativeRawStartAddress_ = 0L;
        }

        super.freeMemory();
    }

    @Override
    protected void finalize() {
        if (requiresFree()) {
            System.err.println(
                    "ERROR: freeMemory() has not been called: Address: " + nativeRawStartAddress_ 
                    + ", capacity: " + capacityBytes_);
            java.lang.StackTraceElement[] arr = Thread.currentThread().getStackTrace();
            for (int i = 0; i < arr.length; i++) {
                System.err.println(arr[i].toString());
            }
        }
    }

    //Restricted methods

    /**
     * Removes existing mapping
     */
    private void unmap() throws RuntimeException {
        try {
            Method method = 
                FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
            method.setAccessible(true);
            method.invoke(fileChannel, nativeRawStartAddress_, capacityBytes_);
            randomAccessFile.close();
        } catch (Exception e) {
            throw new RuntimeException(
                String.format("Encountered %s exception while freeing memory", e.getClass()));
        }
    }

    private static int pageCount(int ps, long length) {
        long s = 1;
        while (s * ps < length) {
            s++;
        }
        return (int)s;
    }

    /**
     * madvise is a system call made by load0 native method
     */
    private void madvise() throws RuntimeException {
        try {
            Class<?> cl = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> ctor = cl.getDeclaredConstructor(
                    int.class,
                    long.class,
                    FileDescriptor.class,
                    Runnable.class);
            ctor.setAccessible(true);
            MappedByteBuffer dbb = (MappedByteBuffer) ctor.newInstance(
                    0, // some junk capacity
                    nativeRawStartAddress_,
                    null,
                    null);
            Method method = MappedByteBuffer.class.getDeclaredMethod("load0", long.class, 
                long.class);
            method.setAccessible(true);
            method.invoke(dbb, nativeRawStartAddress_, capacityBytes_);
        } catch (Exception e) {
            throw new RuntimeException(
                String.format("Encountered %s exception while loading", e.getClass()));
        }
    }

    /**
     * Creates a mapping of the file on disk starting at position and of size length to pages in OS
     * Will throw OutOfMemory error to indicate you've exhausted memory. force garbage collection 
     * and re-attempt.
     */
    private long map(long position, long len) throws RuntimeException {
        int pagePosition = (int)(position % unsafe.pageSize());
        long mapPosition = position - pagePosition;
        long mapSize = len + pagePosition;
        long addr = -1;

        try {
            Method method = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, 
                long.class);
            method.setAccessible(true);
            addr = (long)method.invoke(fileChannel, 1, mapPosition, mapSize);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Encountered %s exception while mapping",
                e.getClass()));
        }

        if (addr < 0) {
            throw new RuntimeException("Negative values are invalid addresses");
        }

        return addr;
    }
}
