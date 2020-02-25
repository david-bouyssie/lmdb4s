package com.github.lmdb4s.bindings



/** This is an opaque Pointer facade for both JNR and Scala Native */
//sealed trait AnyPointer

/*
trait IPointer[P,L] {
  def ptr: Ptr[Byte]
  def length: CSize
}*/

/*
  // TODO: implement this pattern in concrete implementations
  sealed trait IHandle
  type Handle = Ptr[sqlite.sqlite3] with IHandle

  implicit class HandleWrapper(val handle: Handle) extends AnyVal {
    def asPtr(): Ptr[sqlite.sqlite3] = handle.asInstanceOf[Ptr[sqlite.sqlite3]]
  }
 */
