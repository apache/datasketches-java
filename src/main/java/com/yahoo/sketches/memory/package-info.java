/**
 * <p>The memory package contains classes that extend the Java Unsafe class and provide
 * direct primitive and array access to off-heap memory as well as compatible views into heap-based 
 * arrays and the Java ByteBuffer class.  These classes provide the foundation for building and 
 * managing sketches and set operations in off-heap memory.  This package is general purpose,
 * has no external dependencies and can be used in any application that needs to manage data 
 * structures outside the Java heap.
 * </p>
 * 
 * <p>This package detects whether the methods unique to the Unsafe class in JDK8 are present; 
 * if not, methods that are compatible with JDK7 are substituted using an internal
 * interface.  In order for this to work, this library still needs to be compiled using jdk8 
 * but it should be done with both source and target versions of jdk7 specified in pom.xml. 
 * The resultant jar will work on jdk7 and jdk8.</p>
 * 
 * @author Lee Rhodes
 */
package com.yahoo.sketches.memory;