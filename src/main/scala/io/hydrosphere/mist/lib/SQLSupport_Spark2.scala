package io.hydrosphere.mist.lib

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.sql.SparkSession

trait SQLSupport extends SessionSupport {

  private var _session: SparkSession = _

  override def session: SparkSession = _session
  override def spark: SparkSession = _session

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _session = sc.sparkSession
  }
}
