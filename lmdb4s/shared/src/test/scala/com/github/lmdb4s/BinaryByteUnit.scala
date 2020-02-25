package com.github.lmdb4s

object KIBIBYTES {
  private val kb = 1024
  def toBytes(i: Int): Int = i * kb
}

object MEBIBYTES {
  private val mb = 1024 * 1024
  def toBytes(i: Int): Int = i * mb
}