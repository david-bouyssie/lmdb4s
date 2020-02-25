package com.github.lmdb4s

trait IShutdownHookUtil {
  def addShutdownHook(task: () => Unit): Unit
}
