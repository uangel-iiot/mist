package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.FullJobConfiguration

private[mist] object Messages {

  case class CreateContext(namespace: String)

  case class StopAllContexts()

  case class RemoveContext(context: String)

  case class WorkerDidStart(namespace: String, address: String , attributes : Map[String , Any])

  case class GetWorkerInformation()
  case class WorkerInformation(namespace: String, address: String , attributes : Map[String , Any])

  case class AddJobToRecovery(jobId: String, jobConfiguration: FullJobConfiguration)

  case class RemoveJobFromRecovery(jobId: String)

  case class GetWorkerList()

  case class ScalaScript(namespace : String , script : String , sparkConf : Option[Map[String , Any]] = None)
}
