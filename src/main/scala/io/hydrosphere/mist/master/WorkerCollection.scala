package io.hydrosphere.mist.master

import scala.collection.mutable.ArrayBuffer

case class WorkerLink(name: String, address: String, sparkUI : String)

class WorkerCollection {

  class CallbackCollection {

    type NameCallbackPair = (String, WorkerCollection.Callback)

    private val callbacks = ArrayBuffer[NameCallbackPair]()

    def +=(nameCallbackPair: NameCallbackPair): Unit = {
      callbacks += nameCallbackPair
    }

    def -=(nameCallbackPair: NameCallbackPair): Unit = {
      callbacks -= nameCallbackPair
    }

    def apply(name: String): List[WorkerCollection.Callback] = {
      callbacks
        .filter({ (pair) =>
          pair._1 == name
        })
        .map(_._2)
        .toList
    }
  }

  private val workers = scala.collection.mutable.Map[String, WorkerLink]()

  private val callbacks = new CallbackCollection()

  def +=(worker: WorkerLink): Unit = {
    workers += (worker.name -> worker)
    callbacks(worker.name).foreach { (callback) =>
      callback(worker)
      callbacks -= (worker.name, callback)
    }
  }

  def -=(worker: WorkerLink): Unit = {
    workers -= worker.name
  }

  def contains(name: String): Boolean = {
    workers.contains(name)
  }

  def foreach(f: (WorkerLink) => Unit): Unit = {
    workers.foreach {
      case (name, WorkerLink(n ,address , sparkUI)) =>
        f(WorkerLink(name, address , sparkUI))
    }
  }

  def apply(name: String): WorkerLink = {
    val w = workers.get(name)
    w match {
      case Some(v) =>
            v
      case _ =>
            WorkerLink(name , null , null)
    }

  }

  def registerCallbackForName(name: String, callback: WorkerCollection.Callback): Unit = {
    if (workers.contains(name)) {
      callback(this(name))
    } else {
      callbacks += (name -> callback)
    }
  }
}


object WorkerCollection {
  type Callback = (WorkerLink) => Unit

  def apply(): WorkerCollection = {
    new WorkerCollection()
  }
}
