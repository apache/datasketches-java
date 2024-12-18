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

Note that we have a parallel core component for C++ and Python implementations of the same sketch algorithms, 
[datasketches-cpp](https://github.com/apache/datasketches-cpp) and [datasketches-python](https://github.com/apache/datasketches-python)

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---

## Maven Build Instructions
__NOTE:__ This component accesses resource files for testing. As a result, the directory elements of the full absolute path of the target installation directory must qualify as Java identifiers. In other words, the directory elements must not have any space characters (or non-Java identifier characters) in any of the path elements. This is required by the Oracle Java Specification in order to ensure location-independent access to resources: [See Oracle Location-Independent Access to Resources](https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html)

### JDK17 is required to compile
This component depends on the [datasketches-memory-4.1.X](https://github.com/apache/datasketches-memory/tree/4.1.X) component, 
and, as a result, must be compiled with JDK17 and this dependency:

```
<dependency>
  <groupId>org.apache.datasketches</groupId>
  <artifactId>datasketches-memory</artifactId>
  <version>4.1.0</version>
</dependency>
```

If your application only relies on the APIs of datasketches-java no special JVM arguments are required.
However, if your application also directly relies on the APIs of the *datasketches-memory* component, 
you may need the additional JVM argument **--add-modules=jdk.incubator.foreign**.

### Recommended Build Tool
This DataSketches component is structured as a Maven project and Maven is the recommended Build Tool.

To run normal unit tests:

    $ mvn clean test

To install jars built from the downloaded source:

    $ mvn clean install -DskipTests=true

This will create the following jars:

* datasketches-java-X.Y.Z.jar The compiled main class files.
* datasketches-java-X.Y.Z-tests.jar The compiled test class files.
* datasketches-java-X.Y.Z-sources.jar The main source files.
* datasketches-java-X.Y.Z-test-sources.jar The test source files
* datasketches-java-X.Y.Z-javadoc.jar  The compressed Javadocs.

### Dependencies

#### Run-time
There is one run-time dependency: 

* [datasketches-memory-4.1.X](https://github.com/apache/datasketches-memory/tree/4.1.X)

#### Testing
See the pom.xml file for test dependencies.

## Known Issues

#### SpotBugs

* Make sure you configure SpotBugs with the /tools/FindBugsExcludeFilter.xml file. Otherwise, you may get a lot of false positive or low risk issues that we have examined and eliminated with this exclusion file.

