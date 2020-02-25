package com.github.lmdb4s

import java.io.File
import java.nio.ByteBuffer

import scala.scalanative.unsafe._

object Lmdb {

  /**
   * Create an Env using the ByteBufferProxy#PROXY_OPTIMAL.
   *
   * @return the environment (never null)
   */
  def createEnv(): Env.Builder[Array[Byte],Ptr[Byte]] = new Env.Builder[Array[Byte],Ptr[Byte]]()(ByteArrayKeyValFactory)

  /**
   * Create an Env using the passed BufferProxy.
   *
   * @tparam T buffer type
   * @param keyValFactory the KeyVal factory to use (required)
   * @return the environment (never null)
   */
  def createEnv[T >: Null](keyValFactory: IKeyValFactory[T, Ptr[Byte]]): Env.Builder[T,Ptr[Byte]] = {
    new Env.Builder[T, Ptr[Byte]]()(keyValFactory)
  }

  /**
   * Opens an environment with a single default database in 0664 mode using the ByteBufferProxy#PROXY_OPTIMAL.
   *
   * @param path  file system destination
   * @param size  size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   */
  def openEnv(path: File, size: Int, flags: EnvFlags.Flag*): Env[Array[Byte], Ptr[Byte]] = {
    createEnv().setMapSize(size * 1024L * 1024L).open(path, flags: _*)
  }

}
