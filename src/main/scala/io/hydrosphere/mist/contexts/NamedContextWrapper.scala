package io.hydrosphere.mist.contexts

import org.apache.spark.{SparkConf, SparkContext}

private[mist] case class NamedContextWrapper(sparkConf: SparkConf, namespace: String) extends ContextWrapper
