package io.hydrosphere.mist.worker


import java.io.File

import io.hydrosphere.mist.contexts.ContextBuilder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.language.{existentials, implicitConversions}
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.{ConsoleWriter, GenericRunnerSettings, NewLinePrintWriter, Settings}



object InterpreterVariable {


	def createSparkSession(namespace : String) :  SparkSession = {
		ContextBuilder.namedSparkContext(namespace).sparkSession
	}
}

class ScalaInterpreter(namespace : String , sparkConf : SparkConf) {
	val settings = new GenericRunnerSettings( println _ )
	//settings.paret
	settings.embeddedDefaults( Thread.currentThread().getContextClassLoader())
	settings.Yreplclassbased.value = true
	settings.usejavacp.value = true
	settings.Yreploutdir.value = sparkConf.get("spark.repl.class.outputDir")
	settings.outputDirs.setSingleOutput(sparkConf.get("spark.repl.class.outputDir"))
	settings.classpath.value = getUserJars(sparkConf , true).mkString(File.pathSeparator)
	//sys.props("java.class.path")

	//settings..setSingleOutput(contextWrapper.sparkConf.get("spark.repl.class.outputDir"))

	val scalaInterpreter = new ScalaILoop(namespace)
	scalaInterpreter.process(settings)
	//val m = new IMain(settings , new PrintWriter(stringWriter))
	val m = scalaInterpreter.intp

	def unionFileLists(leftList: Option[String], rightList: Option[String]): Set[String] = {
		var allFiles = Set[String]()
		leftList.foreach { value => allFiles ++= value.split(",") }
		rightList.foreach { value => allFiles ++= value.split(",") }
		allFiles.filter { _.nonEmpty }
	}

	def getUserJars(conf: SparkConf, isShell: Boolean = false): Seq[String] = {
		val sparkJars = conf.getOption("spark.jars")
		if (conf.get("spark.master") == "yarn" && isShell) {
			val yarnJars = conf.getOption("spark.yarn.dist.jars")
			unionFileLists(sparkJars, yarnJars).toSeq
		} else {
			sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
		}
	}

	def interpret(code : String) : Result = {
		m.interpret(code)
	}

}
/**
  *  A Spark-specific interactive shell.
  */
class ScalaILoop(namespace : String)
	extends ILoop(new java.io.BufferedReader(new java.io.StringReader("")), new NewLinePrintWriter(new ConsoleWriter, true)) {


	def initializeSpark() {

			processLine(s"""
        @transient val spark = io.hydrosphere.mist.worker.InterpreterVariable.createSparkSession(\"${namespace}\")
		@transient val sc = spark.sparkContext

        """)

			processLine("import org.apache.spark.SparkContext._")
			processLine("import spark.implicits._")
			processLine("import spark.sql")
			processLine("import org.apache.spark.sql.functions._")


	}

	override def loadFiles(settings: Settings): Unit = {
		initializeSpark()
		super.loadFiles(settings)
	}

	override def closeInterpreter() {
	}

}


