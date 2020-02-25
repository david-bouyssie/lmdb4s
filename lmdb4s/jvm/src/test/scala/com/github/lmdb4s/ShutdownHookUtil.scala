package com.github.lmdb4s

object ShutdownHookUtil extends IShutdownHookUtil {
  def addShutdownHook(task: () => Unit): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        task()
      }
    })
  }
}