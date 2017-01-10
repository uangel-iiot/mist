package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, AddressFromURIString, Terminated}
import akka.pattern.ask
import akka.cluster.Cluster
import akka.util.Timeout
import io.hydrosphere.mist.{Logger, MistConfig, Worker}

import scala.concurrent.duration.FiniteDuration
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._

import scala.language.postfixOps
import scala.sys.process._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** Manages context repository */
private[mist] class WorkerManager extends Actor with Logger{

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String): Unit = {
    if (!workers.contains(name)) {
      if (MistConfig.Settings.singleJVMMode) {
        Worker.main(Array(name))
      } else {
        new Thread {
          override def run() = {
            val runOptions = MistConfig.Contexts.runOptions(name)
            val configFile = System.getProperty("config.file")
            val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)

            MistConfig.Workers.runner match {
              case "local" =>
                print("STARTING WORKER: ")
                val cmd: Seq[String] = Seq(
                  s"${sys.env("MIST_HOME")}/bin/mist",
                  "start",
                  "worker",
                  "--runner", "local",
                  "--namespace", name,
                  "--config", configFile.toString,
                  "--jar", jarPath.toString,
                  "--run-options", runOptions)
                cmd !
              case "docker" =>
                val cmd: Seq[String] = Seq(
                  s"${sys.env("MIST_HOME")}/bin/mist",
                  "start",
                  "worker",
                  "--runner", "docker",
                  "--docker-host", MistConfig.Workers.dockerHost,
                  "--docker-port", MistConfig.Workers.dockerPort.toString,
                  "--namespace", name,
                  "--config", configFile.toString,
                  "--jar", jarPath.toString,
                  "--run-options", runOptions)
                cmd !
              case "manual" =>
                Process(
                  Seq("bash", "-c", MistConfig.Workers.cmd),
                  None,
                    "MIST_WORKER_NAMESPACE" -> name,
                    "MIST_WORKER_CONFIG" -> configFile.toString,
                    "MIST_WORKER_JAR_PATH" -> jarPath.toString,
                    "MIST_WORKER_RUN_OPTIONS" -> runOptions
                ).!
            }
          }
        }.start()
      }
    }
  }

  def removeWorkerByName(name: String): Unit = {
    if (workers.contains(name)) {
      val address = workers(name).address
      workers -= name
      cluster.leave(AddressFromURIString(address))
    }
  }
  def removeLocalWorker(): Unit = {
    workers.foreach{
      case WorkerLink(name, address , attributes) =>
        if(address.contains(cluster.selfAddress.host.getOrElse("DummyForNotEqual"))) {
          logger.info(s"cluster.leave for address $name $address")
          cluster.leave(AddressFromURIString(address))
        }
    }
  }

  override def receive: Receive = {
    case CreateContext(name) =>
      startNewWorkerWithName(name)

    // surprise: stops all contexts
    case StopAllContexts =>
      removeLocalWorker()

    // removes context
    case RemoveContext(name) =>
      removeWorkerByName(name)

    case WorkerDidStart(name, address , attributes) =>
      logger.info(s"Worker `$name` did start on $address")
      context watch sender()
      workers += WorkerLink(name, address , attributes)

    case Terminated(actor) =>
      // need to remove worker
      logger.info(s"worker ${actor.path} terminated name ${actor.path.name}, ${actor.path.address}")
      workers -=actor.path.name


    case jobRequest: FullJobConfiguration=>
      val originalSender = sender
      startNewWorkerWithName(jobRequest.namespace)

      workers.registerCallbackForName(jobRequest.namespace, {
        case WorkerLink(name, address , sparkUI) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
            val future = remoteActor.ask(jobRequest)(timeout = FiniteDuration(MistConfig.Contexts.timeout(jobRequest.namespace).toNanos, TimeUnit.NANOSECONDS))
            future.onSuccess {
              case response: Any =>
                if (MistConfig.Contexts.isDisposable(name)) {
                  removeWorkerByName(name)
                }
                originalSender ! response
            }
          }
          else {
            remoteActor ! jobRequest
          }
      })
      
    case scalaScript : ScalaScript => {
      val originalSender = sender
      startNewWorkerWithName(scalaScript.namespace)

      workers.registerCallbackForName(scalaScript.namespace, {
        case WorkerLink(name, address , sparkUI) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          if(MistConfig.Contexts.timeout(scalaScript.namespace).isFinite()) {
            val future = remoteActor.ask(scalaScript)(timeout = FiniteDuration(MistConfig.Contexts.timeout(scalaScript.namespace).toNanos, TimeUnit.NANOSECONDS))
            future.onSuccess {
              case response: Any =>
                if (MistConfig.Contexts.isDisposable(name)) {
                  removeWorkerByName(name)
                }
                originalSender ! response
            }
          }
          else {
            remoteActor ! scalaScript
          }
      })
    }

    case AddJobToRecovery(jobId, jobConfiguration) =>
      if (MistConfig.Recovery.recoveryOn) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.add(jobId, jobConfiguration)
        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
        recoveryActor ! JobStarted
      }

    case RemoveJobFromRecovery(jobId) =>
      if (MistConfig.Recovery.recoveryOn) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.remove(jobId)
        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
        recoveryActor ! JobCompleted
      }

    case GetWorkerList =>
	    val orginalSender = sender
      logger.info("receive get worker list")
      var worker_list = Seq[Future[Any]]()
      workers.foreach{
        case WorkerLink(name ,address , attributes) =>
        {
	        val m =
          if(!attributes.contains("sparkUI") ) {
	          implicit val timeout  =  Timeout(3000 , TimeUnit.MILLISECONDS)
	          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")

	          val future = remoteActor ? GetWorkerInformation
	          future.map{
		          case WorkerInformation(name , address , attributes) =>
			          workers.updateAttributes(name , attributes)
			          Map("name" -> name , "address" -> address , "attributes" -> attributes)
	          }

          } else {
	          Future {
		          Map("name" -> name , "address" -> address , "attributes" -> attributes)
	          }

          }
	        worker_list = m +: worker_list




        }
      }
	    Future.fold(worker_list)(Vector.empty[Any])(_ :+ _ ).onSuccess{
		    case x => {
			    orginalSender ! x
		    }
	    }
  }

}
