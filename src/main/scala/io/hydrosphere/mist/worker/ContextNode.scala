package io.hydrosphere.mist.worker

import java.io.{File, IOException}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.Executors.newFixedThreadPool

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.contexts.{ContextBuilder, ContextWrapper}
import io.hydrosphere.mist.jobs.FullJobConfiguration
import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props, RootActorPath}
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}
import scala.concurrent.ExecutionContext.Implicits.global


class MistClassLoader(urls: Array[URL], parent : ClassLoader) extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = {
    super.addURL(url)
  }

  override def getURLs(): Array[URL] = {
    super.getURLs()
  }
}

class ContextNode(namespace: String) extends Actor with ActorLogging{

  val cl = new MistClassLoader( Array[URL]() , Thread.currentThread().getContextClassLoader)

  Thread.currentThread().setContextClassLoader(cl)

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  val replOutputDir = createTempDir()

  sys addShutdownHook{
    deleteRecursively(replOutputDir)
  }

  lazy val contextWrapper =  ContextBuilder.namedSparkContext(namespace , replOutputDir )
  ContextNode.cw = contextWrapper

  var intp : ScalaInterpreter = null

  var numRequest = 0


  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new java.io.IOException("Failed to create a temp directory (under " + root + ") after " +
            maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    dir.getCanonicalFile
  }


  def createTempDir(root: String = System.getProperty("java.io.tmpdir"),
                    namePrefix: String = "mist"): File = {
    val dir = createDirectory(root, namePrefix)
    dir
  }
  def isSymlink(file: File): Boolean = {
    return Files.isSymbolicLink(Paths.get(file.toURI))
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }



  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }


  def getAttributes() : Map[String , Any] = {

    if(numRequest == 0)
      Map()
    else
      Map("sparkUI" -> contextWrapper.context.uiWebUrl.getOrElse(null))
  }

  log.info(s"outputDir = ${contextWrapper.sparkConf.get("spark.repl.class.outputDir")}")
  override def receive: Receive = {
    case ScalaScript(namespace , script) => {

      var currentRequest = numRequest
      numRequest = numRequest+1

      if(intp == null)
        intp = new ScalaInterpreter(namespace, contextWrapper.sparkConf , cl)

      val b = new java.io.ByteArrayOutputStream()

      Console.withOut(b) {
        intp.interpret(script)

        sender ! b.toString
      }


    }
    case GetWorkerInformation => {
      sender ! WorkerInformation(namespace , cluster.selfAddress.toString , getAttributes())
    }

    case jobRequest: FullJobConfiguration =>

      if(intp == null)
      {
        Thread.currentThread().setContextClassLoader(cl)
      }
      else
      {
        intp.scalaInterpreter.intp.setContextClassLoader()
      }



      var currentRequest = numRequest
      numRequest = numRequest+1

      log.info(s"[WORKER] received JobRequest: $jobRequest")
      //log.info(s"WebUrl = ${contextWrapper.context.uiWebUrl}")
      val originalSender = sender

      lazy val runner = Runner(jobRequest, contextWrapper, cl)

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
          //context.actorSelection(RootActorPath(member.address) / "user" / Constants.Actors.workerManagerName) ! WorkerDidStart(namespace, cluster.selfAddress.toString , contextWrapper.context.uiWebUrl.getOrElse(null) )

        context.actorSelection(RootActorPath(member.address) / "user" / Constants.Actors.workerManagerName) ! WorkerDidStart(namespace, cluster.selfAddress.toString ,  getAttributes() )

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
  var cw : ContextWrapper = null
}