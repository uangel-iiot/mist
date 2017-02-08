package io.hydrosphere.mist.jobs.runners.jar

import java.net.{URL, URLClassLoader}

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobFile}
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.lib.MistJob
import io.hydrosphere.mist.worker.MistClassLoader

private[mist] class JarRunner(jobConfiguration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper, classLoader : MistClassLoader) extends Runner {

  override val configuration: FullJobConfiguration = jobConfiguration

  contextWrapper.addJar(jobFile.file.getPath)

  val cls = {
    //val classLoader = new URLClassLoader(Array[URL](jobFile.file.toURI.toURL), getClass.getClassLoader)
    //val classLoader = Thread.currentThread().getContextClassLoader.asInstanceOf[MistClassLoader]

    classLoader.addURL( jobFile.file.toURI.toURL );
    println("JarRunner class loader = " + getClass.getClassLoader);
    classLoader.loadClass(configuration.className)
  }

  // Scala `object` reference of user job
  val objectRef = cls.getField("MODULE$").get(None)

  // We must add user jar into spark context

  _status = Runner.Status.Initialized

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
    new Thread().run()
    try {
      val result = objectRef match {
        case objectRef: MistJob =>
          objectRef.setup(contextWrapper)
          Left(objectRef.doStuff(configuration.parameters))
        case _ => Right(Constants.Errors.notJobSubclass)
      }

      _status = Runner.Status.Stopped

      result
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = Runner.Status.Aborted
        Right(e.toString)
    }
  }

}
