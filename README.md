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

Note that we have parallel core library components for C++, Python and GO implementations of many of the same sketch algorithms:

- [datasketches-cpp](https://github.com/apache/datasketches-cpp), 
- [datasketches-python](https://github.com/apache/datasketches-python),
- [datasketches-go](https://github.com/apache/datasketches-go).

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information.

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.


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
* Your default \$JAVA\_HOME compiler must be OpenJDK compatible, specified in the toolchain, and may be a version greater than 25. Note that if your \$JAVA\_HOME is set to a Java version greater than 25, Maven will automatically use the Java 25 version specified in the toolchain instead. The pom.xml specifies any necessary JVM flags, if required, so no further action is needed.
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

### Checkstyle

* At the time of this writing, Checkstyle had not been upgraded to handle Java 25 features. 

