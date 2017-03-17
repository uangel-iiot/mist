package io.hydrosphere.mist.worker


import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.HDFSJobFile
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

class ScalaInterpreter(namespace : String , sparkConf : SparkConf ,  classLoader : ClassLoader , userJars : Option[Seq[String]] =None) {

	val settings = new GenericRunnerSettings( println _ )
	//settings.paret
	println("scala interpreter embed defauls. cl = " + classLoader);
	settings.embeddedDefaults( classLoader )
	settings.Yreplclassbased.value = true
	settings.usejavacp.value = true
	settings.Yreploutdir.value = sparkConf.get("spark.repl.class.outputDir")
	settings.outputDirs.setSingleOutput(sparkConf.get("spark.repl.class.outputDir"))

	settings.classpath.value = getUserJars(sparkConf , userJars, true).mkString(File.pathSeparator)

	println("interpreter settings.classpath.value = " + settings.classpath.value)
	//sys.props("java.class.path")

	//settings..setSingleOutput(contextWrapper.sparkConf.get("spark.repl.class.outputDir"))

	val scalaInterpreter = new ScalaILoop(namespace)
	scalaInterpreter.process(settings)
	//val m = new IMain(settings , new PrintWriter(stringWriter))
	val m = scalaInterpreter.intp

	/*
	if(classLoader.isInstanceOf[URLClassLoader])
		classLoader.asInstanceOf[URLClassLoader].getURLs.foreach((u) => {
			println("add url  u = " + u)
			m.addUrlsToClassPath(u)
		})
	*/

	def unionFileSeq(leftList : Seq[String] , rightList : Option[Seq[String]]) : Set[String] = {
		var allFiles = Set[String]()
		allFiles ++= leftList
		rightList.foreach { value => allFiles ++= value }

		allFiles.filter { _.nonEmpty }
	}

	def unionFileLists(leftList: Option[String], rightList: Option[String]): Set[String] = {
		var allFiles = Set[String]()
		leftList.foreach { value => allFiles ++= value.split(",") }
		rightList.foreach { value => allFiles ++= value.split(",") }
		allFiles.filter { _.nonEmpty }
	}

	def getUserJars(conf: SparkConf, userJars : Option[Seq[String]], isShell: Boolean = false): Seq[String] = {
		val sparkJars = conf.getOption("spark.jars")

		val uj = userJars.map{_.map{
			(path) => {
				println(s"path = ${path}")
				if( path.startsWith("hdfs:/")) {
					val hdfsJobFile = new HDFSJobFile(path)
					println(s"hdfsJobFile = ${hdfsJobFile.file.getPath}")
					hdfsJobFile.file.getPath
				}
				else {
					path
				}
			}
		}}
		var ret = sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
		if (conf.get("spark.master") == "yarn" && isShell) {
			val yarnJars = conf.getOption("spark.yarn.dist.jars")
			ret = unionFileSeq(ret, yarnJars.map(_.split(","))).toSeq
		}
		if(classLoader.isInstanceOf[URLClassLoader])
		{
			classLoader.asInstanceOf[URLClassLoader].getURLs.foreach(u => println("Mist Url Class Loader jar = " + u))
			ret = unionFileSeq(ret , Some(classLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toString).toSeq)).toSeq

		}

		unionFileSeq(ret , uj).toSeq
	}

	def interpret(code : String) : Unit = {
		m.interpret(code)
		//scalaInterpreter.processLine(code)

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