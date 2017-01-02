package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.FullJobConfiguration
import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props, RootActorPath}
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}


import scala.concurrent.ExecutionContext.Implicits.global


class ContextNode(namespace: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(namespace)

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case jobRequest: FullJobConfiguration =>
      log.info(s"[WORKER] received JobRequest: $jobRequest")
      //log.info(s"WebUrl = ${contextWrapper.context.uiWebUrl}")
      val originalSender = sender

      lazy val runner = Runner(jobRequest, contextWrapper)

      val future: Future[Either[Map[String, Any], String]] = Future {
        if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite())
          serverActor ! AddJobToRecovery(runner.id, runner.configuration)
        log.info(s"${jobRequest.namespace}#${runner.id} is running")

        runner.run()
      }(executionContext)
      future
        .recover {
          case e: Throwable => originalSender ! Right(e.toString)
        }(ExecutionContext.global)
        .andThen {
          case _ =>
            if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite())
              serverActor ! RemoveJobFromRecovery(runner.id)
        }(ExecutionContext.global)
        .andThen {
          case Success(result: Either[Map[String, Any], String]) => originalSender ! result
          case Failure(error: Throwable) => originalSender ! Right(error.toString)
        }(ExecutionContext.global)

    case MemberUp(member) =>
      if ( member.hasRole(Constants.Actors.workerManagerName)) {
        //if ( member.address.host.getOrElse("Master") == cluster.selfAddress.host.getOrElse("Self") ) {
          context.actorSelection(RootActorPath(member.address) / "user" / Constants.Actors.workerManagerName) ! WorkerDidStart(namespace, cluster.selfAddress.toString , contextWrapper.context.uiWebUrl.getOrElse(null) )
        //}
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        //cluster.system.shutdown()
      }

    case MemberRemoved(member, prevStatus) =>
      if (member.address == cluster.selfAddress) {

        /*
        log.info("cluster system shutdown!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        cluster.system.shutdown()
        log.info("await Termination !!!!!!!!!!! ")


        cluster.system.awaitTermination()
        log.info("system Exit !!!!!!!!!!!")
        sys.exit(0)
        */

        val future = cluster.system.terminate()
        future.onSuccess {
          case terminated =>
            sys.exit(0)
        }


      }
  }
}

object ContextNode {
  def props(namespace: String): Props = Props(classOf[ContextNode], namespace)
}