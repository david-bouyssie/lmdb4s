package com.github.lmdb4s

import java.io.File
import java.lang.Integer.BYTES
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect

trait AbstractTestUtils {

  val DB_1 = "test-db-1"

  //@SuppressWarnings(Array("PMD.AvoidUsingOctalValues"))
  val POSIX_MODE: Int = Integer.parseInt("0664") // "0644" could be fine

  private val randomGen = new scala.util.Random()

  private[lmdb4s] val shutdownHookUtil: IShutdownHookUtil

  private[lmdb4s] def ba(value: Int): Array[Byte]

  private[lmdb4s] def bb(value: Int): ByteBuffer = {
    val bb = allocateDirect(BYTES)
    bb.putInt(value).flip()
    bb
  }

  private[lmdb4s] def className(clazz: Class[_]): String = {
    val name = clazz.getName
    name.substring(name.lastIndexOf('.') + 1)
  }

  private[lmdb4s] def deleteTempFileThenCheck(file: File, isDir: Boolean = false): Boolean = {
    val label = if (isDir) "dir" else "file"

    val wasDeleted = file.delete()
    if (wasDeleted) println(s"Deleted TEMP $label: $file")
    else println(s"Can't delete TEMP $label: $file")

    wasDeleted
  }

  private[lmdb4s] def createTempDirectory(prefix: String, deleteDirOnExit: Boolean = true): File = {
    val dirName = s"${prefix}_${System.currentTimeMillis()}_${randomGen.nextInt(1000)}_tmpdir"

    val dir = new File(s"./target/tests/$dirName").getCanonicalFile
    assert(!dir.exists(), s"temporary directory '$dir' already exists")
    dir.mkdirs()

    println("Created TEMP dir: " + dir)

    if (deleteDirOnExit) {
      shutdownHookUtil.addShutdownHook { () =>

        // Delete TEMP directory content
        val files = dir.listFiles()

        if (files != null ) {
          files.foreach(deleteTempFileThenCheck(_))
        }

        // Delete TEMP directory
        deleteTempFileThenCheck(dir, true)
      }
    }

    dir
  }

  private[lmdb4s] def createTempLmdbFile(prefix: String, deleteLockFileOnExit: Boolean): File = {
    val fileName = s"${prefix}_${System.currentTimeMillis()}_${randomGen.nextInt(1000)}_tmpfile"

    val file = new File(s"./target/tests/$fileName").getCanonicalFile
    assert(!file.exists(), s"temporary file '$file' already exists")

    println("Created TEMP file: " + file)

    shutdownHookUtil.addShutdownHook { () =>

      // Delete TEMP ldmb file
      deleteTempFileThenCheck(file)

      // Delete TEMP lock file
      if (deleteLockFileOnExit)
        deleteTempFileThenCheck(new File(s"${file}-lock"))
    }

    file
  }
}
