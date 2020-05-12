package com.github.lmdb4s

import java.io.File
import java.util.Comparator

trait ICursor[T] extends AutoCloseable {

  def close(): Unit

  def count(): Long

  def delete(f: PutFlags.Flag*): Unit

  def first(): Boolean

  def get(key: T, op: GetOp.Op): Boolean

  def key: T

  def getKeyAsBytes(): Array[Byte]

  def last(): Boolean

  def next(): Boolean

  def prev(): Boolean

  def put(key: T, `val`: T, op: PutFlags.Flag*): Boolean

  def putMultiple(key: T, `val`: T, elements: Int, op: PutFlags.Flag*): Unit

  def renew(newTxn: ITxn[T]): Unit

  def reserve(key: T, size: Int, op: PutFlags.Flag*): T

  def seek(op: SeekOp.Op): Boolean

  def `val`: T
}

trait ICursorIterator[T] extends java.util.Iterator[CursorIterator.KeyVal[T]] with AutoCloseable {
  def iterable(): java.lang.Iterable[CursorIterator.KeyVal[T]]
}

trait IDbi[T] {

  def close(): Unit

  def delete(key: T): Boolean

  def delete(txn: ITxn[T], key: T): Boolean

  def delete(txn: ITxn[T], key: T, `val`: T): Boolean

  def drop(txn: ITxn[T]): Unit

  def drop(txn: ITxn[T], delete: Boolean): Unit

  def get(txn: ITxn[T], key: T): T

  def getName(): Array[Byte]

  def iterate(txn: ITxn[T]): ICursorIterator[T]

  def iterate(txn: ITxn[T], range: KeyRange[T]): ICursorIterator[T]

  def iterate(txn: ITxn[T], range: KeyRange[T], comparator: Comparator[T]): ICursorIterator[T]

  def openCursor(txn: ITxn[T]): ICursor[T]

  def put(key: T, `val`: T): Unit

  def put(txn: ITxn[T], key: T, `val`: T, flags: PutFlags.Flag*): Boolean

  def reserve(txn: ITxn[T], key: T, size: Int, op: PutFlags.Flag*): T

  def stat(txn: ITxn[T]): Stat
}

trait IEnv[T] extends AutoCloseable {
  override def close(): Unit

  def copy(path: File, flags: CopyFlags.Flag*): Unit

  def getDbiNames(): Seq[Array[Byte]]

  def getDbiNames(charset: java.nio.charset.Charset): Seq[String]

  def setMapSize(mapSize: Long): Unit

  def getMaxKeySize(): Int

  def info(): EnvInfo

  def isClosed: Boolean

  def isReadOnly: Boolean

  def openDbi(name: String, flags: DbiFlags.Flag*): IDbi[T]

  def openDbi(name: String, comparator: Comparator[T], flags: DbiFlags.Flag*): IDbi[T]

  def openDbi(name: Array[Byte], flags: DbiFlags.Flag*): IDbi[T]

  def openDbi(name: Array[Byte], comparator: Comparator[T], flags: DbiFlags.Flag*): IDbi[T]

  def stat(): Stat

  def sync(force: Boolean): Unit

  def txn(parent: ITxn[T], flags: TxnFlags.Flag*): ITxn[T]

  def txnRead(): ITxn[T]

  def txnWrite(): ITxn[T]
}

trait ITxn[T] extends AutoCloseable {
  def abort(): Unit

  def close(): Unit

  def commit(): Unit

  def getId: Long

  def getParent: ITxn[T]

  def isReadOnly: Boolean

  def key: T

  def renew(): Unit

  def reset(): Unit

  def value: T
}