package com.github.lmdb4s

object ShutdownHookUtil extends IShutdownHookUtil {
  def addShutdownHook(task: () => Unit): Unit = scala.scalanative.runtime.ShutdownHookProxy.addHook(task)
}
