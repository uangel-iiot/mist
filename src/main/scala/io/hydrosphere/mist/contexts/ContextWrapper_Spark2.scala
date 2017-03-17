package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  private var isHive = false

  lazy val sparkSession = {
     var builder = SparkSession
      .builder()
      .appName(context.appName)
      .config(context.getConf)
     if (isHive) {
       builder = builder.enableHiveSupport()
     }
     builder.getOrCreate()
  }

  def withHive(): ContextWrapper = {
    isHive = true
    this
  }

  lazy val context = {
    SparkContext.getOrCreate(sparkConf)
  }

  def javaContext: JavaSparkContext = new JavaSparkContext(context)

  def sparkConf: SparkConf

  def addJar(jarPath: String): Unit = {
    val jarAbsolutePath = if(jarPath.startsWith("hdfs://")) jarPath else new File(jarPath).getAbsolutePath
    if (!jars.contains(jarAbsolutePath)) {
      context.addJar(jarPath)
      jars += jarAbsolutePath
    }
  }

  def stop(): Unit = {
    context.stop()
  }

  def getJars() : Option[Seq[String]] = {
    if( jars.size == 0)
      None
    else
      Some(jars.toSeq)
  }
}
