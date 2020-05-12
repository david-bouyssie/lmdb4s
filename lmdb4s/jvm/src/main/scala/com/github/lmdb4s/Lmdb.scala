package com.github.lmdb4s

import java.io.File
import java.nio.ByteBuffer

import jnr.ffi.{ Pointer => JnrPointer }

object Lmdb {

  /**
   * Create an Env using the ByteBufferProxy#PROXY_OPTIMAL.
   *
   * @return the environment (never null)
   */
  def createEnv(): Env.Builder[ByteBuffer,JnrPointer] = new Env.Builder[ByteBuffer,JnrPointer]()(ByteBufferKeyValFactory)

  /**
   * Create an Env using the passed BufferProxy.
   *
   * @tparam T buffer type
   * @param keyValFactory the KeyVal factory to use (required)
   * @return the environment (never null)
   */
  def createEnv[T >: Null](keyValFactory: IKeyValFactory[T, JnrPointer]): Env.Builder[T,JnrPointer] = {
    new Env.Builder[T, JnrPointer]()(keyValFactory)
  }

  /**
   * Opens an environment with a single default database in 0664 mode using the ByteBufferProxy#PROXY_OPTIMAL.
   *
   * @param path  file system destination
   * @param size  size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   */
  def openEnv(path: File, size: Int, flags: EnvFlags.Flag*): IEnv[ByteBuffer] = {
    createEnv().setMapSize(size * 1024L * 1024L).open(path, flags: _*)
  }

}
