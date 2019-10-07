<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

[![Build Status](https://travis-ci.org/apache/incubator-datasketches-java.svg?branch=master)](https://travis-ci.org/apache/incubator-datasketches-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/incubator-datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/incubator-datasketches-java/context:java)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/apache/incubator-datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/incubator-datasketches-java/alerts/)
[![Coverage Status](https://coveralls.io/repos/github/apache/incubator-datasketches-java/badge.svg?branch=master&service=github)](https://coveralls.io/github/apache/incubator-datasketches-java?branch=master)

=================

# DataSketches Core Java Component
This is the core Java component of the DataSketches library.  It contains all of the sketching algorithms and can be accessed directly from user applications. 
This component is also is a dependency of other components of the library that create adaptors for target systems, such as Hadoop Pig and Hadoop Hive.
Please refer to our [website](https://datasketches.github.io) for more comprehensive information about the various sketching algorithms and how to use them.

Note that we have a parallel core component for C++ and Python implementations of the same sketch algorithms, 
[incubator-datasketches-cpp](https://github.com/apache/incubator-datasketches-cpp).

## Documentation

### [DataSketches Library Website](https://datasketches.github.io/)

### [Java Core Overview](https://datasketches.github.io/docs/TheChallenge.html)

### [Java Core Javadocs](https://datasketches.github.io/api/core/snapshot/apidocs/index.html)

## Downloading Latest Release
__NOTE:__ This component accesses resource files for testing. As a result, the directory elements of the full absolute path of the target installation directory 
    must qualify as Java identifiers. In other words, the directory elements must not have any space characters (or non-Java identifier characters) in any of the path elements.
    
This is required by the Oracle Java Specification in order to ensure location-independent 
    access to resources: [See Oracle Location-Independent Access to Resources](https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html)

### [Zip File from Apache Archive](http://archive.apache.org/dist/incubator/datasketches/java/)

### [Jar Files from Maven Central](https://repository.apache.org/content/repositories/releases/org/apache/datasketches/datasketches-java/)

### [GitHub](https://github.com/apache/incubator-datasketches-java/releases)

## Build Instructions

### JDK8 is Required Compiler
This DataSketches component is pure Java and you must compile using JDK 8.

### Recommended Build Tool
The DataSketches-java component structured as a Maven project and Maven is the recommended Build Tool.

There are two types of tests: normal unit tests and tests run by the strict profile.  

To run normal unit tests:

    $ mvn clean test

To run the strict profile tests:

    $ mvn clean test -P strict

To install jars built from the downloaded source:

    $ mvn clean install -DskipTests=true

This will create the following jars:

* datasketches-java-X.Y.Z-incubating.jar The compiled main class files.
* datasketches-java-X.Y.Z-incubating-tests.jar The compiled test class files.
* datasketches-java-X.Y.Z-incubating-sources.jar The main source files.
* datasketches-java-X.Y.Z-incubating-test-sources.jar The test source files
* datasketches-java-X.Y.Z-incubating-javadoc.jar  The compressed Javadocs.

### Dependencies

#### Run-time
There is one run-time dependency: 

* org.apache.datasketches : datasketches-memory

#### Testing
See the pom.xml file for test dependencies.

## Resources

### [Issues for datasketches-java](https://github.com/apache/incubator-datasketches-java/issues)

### [Forum](https://groups.google.com/forum/#!forum/sketches-user)

### [Dev mailing list](dev@datasketches.apache.org)
