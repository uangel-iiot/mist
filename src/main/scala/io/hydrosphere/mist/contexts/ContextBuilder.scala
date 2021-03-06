package io.hydrosphere.mist.contexts

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util.UUID

import io.hydrosphere.mist.MistConfig
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.{SparkConf, SparkContext}

/** Builds spark contexts with necessary settings */
private[mist] object ContextBuilder {

  var wrapper : ContextWrapper = null


  /** Build contexts with namespace
    *
    * @param namespace namespace
    * @return [[ContextWrapper]] with prepared context
    */
  def namedSparkContext(namespace: String , replOutputDir : File = null , userConf : Option[Map[String,Any]] = None ): ContextWrapper = {

    if(wrapper == null)
    {

      val sparkConf = new SparkConf()
          .setAppName(namespace)
          .set("spark.driver.allowMultipleContexts", "true")
      if(replOutputDir!=null)
          sparkConf.set("spark.repl.class.outputDir", replOutputDir.getAbsolutePath())

      val sparkConfSettings = MistConfig.Contexts.sparkConf(namespace)

      for (keyValue: List[String] <- sparkConfSettings) {
          println(s"set spark conf ${keyValue.head} = ${keyValue(1)}")
          sparkConf.set(keyValue.head, keyValue(1))
      }


        userConf.foreach {
            _.foreach{
                case (key,value) => {
                    sparkConf.set(key , value.toString)
                }
            }
        }


      wrapper = NamedContextWrapper(sparkConf, namespace)

       userConf.foreach{
           _.get("mist.worker.jars").foreach{
               case jars : Seq[String] => {
                    jars.foreach { j =>
                        println(s"add jar ${j}")
                        wrapper.addJar(j)
                    }
               }
           }
       }
    }

    wrapper

  }
}

