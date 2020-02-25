package scala.scalanative.runtime

object ShutdownHookProxy {
  @inline def addHook(task: () => Unit): Unit = {
    scala.scalanative.runtime.Shutdown.addHook(task)
  }
}
