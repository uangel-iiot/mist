node
{
  currentBuild.result = "SUCCESS"

  try {
    stage 'clone projet'
      checkout scm

    stage 'build and test'
      parallel ( failFast: false,
          Spark_1_5_2: { test_mist("1.5.2") },
          Spark_1_6_2: { test_mist("1.6.2") },
          Spark_2_0_0: { test_mist("2.0.0") },
      )
  }
  catch (err) {

    currentBuild.result = "FAILURE"
    echo "${err}"
    mail body: "project build error is here: ${env.BUILD_URL}" ,
        from: 'hydro-support@provectus.com',
        replyTo: 'noreply@provectus.com',
        subject: 'project build failed',
        to: "peanig@gmail.com"
    throw err
  }
}

def test_mist(sparkVersion)
{
  echo 'prepare for Mist with Spark version - ' + sparkVersion  
  sh "export mistVolume=`docker create -v /usr/share/mist hydrosphere/mist:tests-${sparkVersion}`"
  sh "export mosquittoId=`docker run -d ansi/mosquitto`"
  sh "export hdfsId=`docker run --volumes-from $mistVolume -d hydrosphere/hdfs start`"
  
  echo 'test Mist with Spark version - ' + sparkVersion
  sh "export mistId=`docker run --link $mosquittoId:mosquitto --link $hdfsId:hdfs -v $PWD:/usr/share/mist hydrosphere/mist:tests-${sparkVersion} tests`"
  
  echo 'remove containers'
  sh "docker rm -f $mosquittoId"
  sh "docker rm -f $hdfsId"
  sh "docker rm -f $mistId"
  sh "docker rm -f $mistVolume"
}
