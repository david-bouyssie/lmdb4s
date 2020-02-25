# LMDB4S

LMDB4S is port of the Java library [LmdbJava](https://github.com/lmdbjava/lmdbjava) for the Scala Native platform.

The goal of this project is to provide a thin wrapper around the LMDB C library with an API similar to the lmdbjava one.

# Current status of the project

This is the first release of this library. The JVM version should have a behavior similar to the lmdbjava one.
However the SN (Scala Native) implementation is still unstable. You have been warned ;-)
The long-term goal is to provide similar features, API and stability for both the JVM and SN implementations.

Please, also note that this library is not yet released on maven central, so to use it, you will have to clone this repository and publishLocal from sbt.

## Getting started
<!-- [![Maven Central](https://img.shields.io/maven-central/v/com.github.david-bouyssie/sqlite4s_native0.3_2.11/0.1.0)](https://mvnrepository.com/artifact/com.github.david-bouyssie/sqlite4s_native0.3_2.11/0.1.0) -->

If you are already familiar with Scala Native you can jump right in by adding the following dependency in your `sbt` build file.

```scala
libraryDependencies += "com.github.david-bouyssie" %%% "lmdb4s" % "x.y.z"
```

To use in `sbt`, replace `x.y.z` with the latest version number (currently 0.1.0-SNAPSHOT).

<!-- To use in `sbt`, replace `x.y.z` with the version from Maven Central badge above.
     All available versions can be seen at the [Maven Repository](https://mvnrepository.com/artifact/com.github.david-bouyssie/sqlite4s). -->

If you are not familiar with Scala Native, please follow the relative [Getting Started](https://scala-native.readthedocs.io/en/latest/user/setup.html) instructions.

Additionally, you need to install [LMDB](https://symas.com/lmdb/) on you system as follows:

* Linux/Ubuntu

```
$ sudo apt-get install liblmdb-dev
```

* macOS

```
$ brew install lmdb
```

* Other OSes need to have `liblmdb` available on the system.
An alternative could consist in creating a project sub-directory called for instance "native-lib" and to put the LMDB shared library in this directory.
Then you would also have to change the build.sbt file and add the following settings:
```
nativeLinkingOptions ++= Seq("-L" ++ baseDirectory.value.getAbsolutePath() ++ "/native-lib")
```

## Useful links:

* [LMDB C API](http://www.lmdb.tech/doc/index.html)
* [LmdbJava](https://github.com/lmdbjava/lmdbjava)
* [LMDB github organization](https://github.com/LMDB)
* [Tutorial: a short guide to LMDB](https://blogs.kolabnow.com/2018/06/07/a-short-guide-to-lmdb)