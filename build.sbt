import scala.language.postfixOps
import scala.sys.process._

val sharedSettings = Seq(
  name := "lmdb4s",
  organization := "com.github.david-bouyssie",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.12",
  testFrameworks += new TestFramework("utest.runner.Framework")
)

lazy val makeLibraries = taskKey[Unit]("Building native components")

lazy val lmdb4s = crossProject(JVMPlatform, NativePlatform)
  .crossType(CrossType.Full)
  .in(file("lmdb4s"))
  .settings(sharedSettings)
  /*.jsSettings(/* ... */) // defined in sbt-scalajs-crossproject */
  .jvmSettings(
    libraryDependencies ++= Seq(
      // See: https://github.com/zakgof/java-native-benchmark
      "com.github.jnr" % "jnr-ffi" % "2.1.11",
      "com.github.jnr" % "jffi" % "1.2.22",
      "com.github.jnr" % "jffi" % "1.2.22" classifier "native",
      "com.github.jnr" % "jnr-constants" % "0.9.14",

      "org.lmdbjava" % "lmdbjava-native-linux-x86_64" % "0.9.23-1",
      "org.lmdbjava" % "lmdbjava-native-osx-x86_64" % "0.9.23-1",
      "org.lmdbjava" % "lmdbjava-native-windows-x86_64" % "0.9.23-1",

      "org.agrona" % "agrona" % "1.2.0" % "test",
      "io.netty" % "netty-buffer" % "4.1.44.Final" % "test",

      //"biz.enef" %%% "slogging" % "0.6.1",
      "com.lihaoyi" %%% "utest" % "0.6.8" % "test"
    ),
    fork := true
  )
  // configure Scala-Native settings
  .nativeSettings( // defined in sbt-scala-native

    /*
    // Replace the default "lib" folder by "jars", so that we can use "lib" for native libraries
    unmanagedBase := baseDirectory.value / "jars",

    // Configure the task which will build native libraries from C source code
    // See:
    // - https://binx.io/blog/2018/12/08/the-scala-build-tool/
    // - https://stackoverflow.com/questions/24996437/how-to-execute-a-bash-script-as-sbt-task/25005651
    makeLibraries := {

      val s: TaskStreams = streams.value

      // Create lib directory
      val libDir = (baseDirectory.value / "lib")
      libDir.mkdir()

      s.log.info("Building native libraries...")
      val strBuilderBuild: Int = if (libDir / "libstrbuilder.so" exists()) 0 else "make --print-directory --directory=./io/native/c/strbuilder/" !<

      if(strBuilderBuild == 0) {
        s.log.success("Native libraries were successfully built!")
      } else {
        throw new IllegalStateException("can't build native libraries!")
      }
    },

    (compile in Compile) := ((compile in Compile) dependsOn makeLibraries).value,

    // Add the lib folder to the cleanFiles list (issue: the lib folder itself is deleted)
    // See: https://stackoverflow.com/questions/10471596/add-additional-directory-to-clean-task-in-sbt-build
    cleanFiles += baseDirectory.value / "lib",
    
    // Link custom native libraries
    nativeLinkingOptions ++= Seq("-L" ++ baseDirectory.value.getAbsolutePath() ++ "/lib"),
    */

    // Customize Scala Native settings
    nativeLinkStubs := true, // Set to false or remove if you want to show stubs as linking errors
    nativeMode := "debug", //"debug", //"release-fast", //"release-full",
    nativeLTO := "thin",
    nativeGC := "immix", //"boehm",

    // TODO: remove me when SN deps are available for SN 0.4.x
    libraryDependencies ++= Seq(
      //"biz.enef" %%% "slogging" % "0.6.2-SNAPSHOT", // TODO: publishLocal with version 0.6.1
      "com.lihaoyi" %%% "utest" % "0.7.4" % "test"
    )
  )

lazy val lmdb4sJVM    = lmdb4s.jvm
lazy val lmdb4sNative = lmdb4s.native

// TODO: uncomment this when ready for publishing
/*
val publishSettings = Seq(

  // Your profile name of the sonatype account. The default is the same with the organization value
  sonatypeProfileName := "david-bouyssie",

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/david-bouyssie/lmdb4s"),
      "scm:git@github.com:david-bouyssie/lmdb4s.git"
    )
  ),

  developers := List(
    Developer(
      id    = "david-bouyssie",
      name  = "David Bouyssié",
      email = "",
      url   = url("https://github.com/david-bouyssie")
    )
  ),
  description := "",
  licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")), // FIXME: update license
  homepage := Some(url("https://github.com/david-bouyssie/lmdb4s")),
  pomIncludeRepository := { _ => false },
  publishMavenStyle := true,

  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },

  // Workaround for issue https://github.com/sbt/sbt/issues/3570
  updateOptions := updateOptions.value.withGigahorse(false),

  useGpg := true,
  pgpPublicRing := file("~/.gnupg/pubring.kbx"),
  pgpSecretRing := file("~/.gnupg/pubring.kbx"),

  Test / skip in publish := true
)*/
