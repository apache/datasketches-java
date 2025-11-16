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

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java)
[![Coverage Status](https://coveralls.io/repos/github/apache/datasketches-java/badge.svg)](https://coveralls.io/github/apache/datasketches-java)

=================

# Apache<sup>&reg;</sup> DataSketches&trade; Core Java Library Component
This is the core Java component of the DataSketches library.  It contains all of the sketching algorithms and can be accessed directly from user applications. 

This component is also a dependency of other components of the library that create adaptors for target systems, such as the [Apache Pig adaptor](https://github.com/apache/datasketches-pig), the [Apache Hive adaptor](https://github.com/apache/datasketches-hive), and others.

Note that we have parallel core components for C++, Python and GO implementations of many of the same sketch algorithms: 

- [datasketches-cpp](https://github.com/apache/datasketches-cpp), 
- [datasketches-python](https://github.com/apache/datasketches-python),
- [datasketches-go](https://github.com/apache/datasketches-go).

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information.

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---
## Major Changes with this Release
This release is a major release where we took the opportunity to do some significant refactoring that will constitute incompatible changes from previous releases.  Any incompatibility with prior releases is always an inconvenience to users who wish to just upgrade to the latest release and run.  However, some of the code in this library was written in 2013 and meanwhile the Java language has evolved enormously since then.  We chose to use this major release as the opportunity to modernize some of the code to achieve the following goals:

### Eliminate the dependency on the DataSketches-Memory component.  
The DataSketches-Memory component was originally developed in 2014 to address the need for fast access to off-heap memory data structures and used Unsafe and other JVM internals as there were no satisfactory Java language features to do this at the time. 

The FFM capabilities introduced into the language in Java 22, are now part of the Java 25 LTS release, which we support. Since the capabilities of FFM are a superset of the original DataSketches-Memory component, it made sense to rewrite the code to eliminate the dependency on DataSketches-Memory and use FFM instead.  This impacted code across the entire library.

This provided several advantages to the code base. By removing this dependency on DataSketches-Memory, there are now no runtime dependencies! This should make integrating this library into other Java systems much simpler. Since FFM is tightly integrated into the Java language, it has improved performance, especially with bulk operations.

- As an added note: There are numerous other improvements to the Java language that we could perhaps take advantage of in a rewrite, e.g., Records, text blocks, switch expressions, sealed, var, modules, patterns, etc.  However, faced with the risk of accidentally creating bugs due to too many changes at one time, we focused on FFM, which actually improve performance as opposed to just syntactic sugar.

### Align public sketch class names so that the sketch family name is part of the class name.
For example, the Theta sketch was the first sketch written for the library and its base class was called *Sketch*.  Obviously, because it was the only sketch! The Tuple sketch evolved soon after and its base class was also called *Sketch*.  Oops, bad idea. If a user wanted to use both the Theta and Tuple sketches in the same class one of them had to be fully qualified every time it was referenced. Ugh! 

Unfortunately, this habit propagated so some of the other early sketches where we ended up with two different sketches with a *ItemsSketch*, for example. For the more recent additions to the library we started including the sketch family name in all the relevant sketch-like public classes of a sketch family.

In this release we have refactored these older sketches with new names that now include the sketch family name.  Yes, this is an incompatible change for user code moving from earlier releases, but this can be usually fixed with search-and-replace tools. This release is not perfect, but hopefully more consistent across all the different sketch families.


## Build & Runtime Dependencies

### Installation Directory Path
**NOTE:** This component accesses resource files for testing. As a result, the directory elements of the full absolute path of the target installation directory must qualify as Java identifiers. In other words, the directory elements must not have any space characters (or non-Java identifier characters) in any of the path elements. This is required by the Oracle Java Specification in order to ensure location-independent access to resources: [See Oracle Location-Independent Access to Resources](https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html)

### OpenJDK Version 25
At minimum, an OpenJDK-compatible build of Java 25, provided by one of the Open-Source JVM providers, such as *Azul Systems*, *Red Hat*, *SAP*, *Eclipse Temurin*, etc, is required.
All of the testing of this release has been performed with the *Eclipse Temurin* build.

## Compilation and Test using Maven
This DataSketches component is structured as a Maven project and Maven is the recommended tool for compile and test.

#### A Toolchain is required

* You must have a JDK type toolchain defined in location *~/.m2/toolchains.xml* that specifies where to find a locally installed OpenJDK-compatible version 25.
* Your default \$JAVA\_HOME compiler must be OpenJDK compatible, specified in the toolchain, and may be a version greater than 25. Note that if your \$JAVA\_HOME is set to a Java version greater than 25, Maven will automatically use the Java 25 version specified in the toolchain instead. The included pom.xml specifies the necessary JVM flags, if required, so no further action is needed.
* Note that the paths specified in the toolchain must be fully qualified direct paths to the OpenJDK version locations. Using environment variables will not work.

#### To run normal unit tests:

    $ mvn clean test

#### To install jars built from the downloaded source:

    $ mvn clean install -DskipTests=true

This will create the following jars:

* datasketches-java-X.Y.Z.jar The compiled main class files.
* datasketches-java-X.Y.Z-tests.jar The compiled test class files.
* datasketches-java-X.Y.Z-sources.jar The main source files.
* datasketches-java-X.Y.Z-test-sources.jar The test source files
* datasketches-java-X.Y.Z-javadoc.jar  The compressed Javadocs.

## Known Issues

### SpotBugs

* Make sure you configure SpotBugs with the /tools/FindBugsExcludeFilter.xml file. Otherwise, you may get a lot of false positive or low risk issues that we have examined and eliminated with this exclusion file.

