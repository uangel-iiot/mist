node
{
  currentBuild.result = "SUCCESS"
  
  stage 'clone projet(test3)'
    checkout scm

  try {
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
//  def mosquittoId = docker.image('ansi/mosquitto:latest').run().id
//  def mistVolume = docker.image("hydrosphere/mist:tests-${sparkVersion}").run("-v /usr/share/mist").id
//  def hdfsId = docker.image('hydrosphere/hdfs:latest').run("--volumes-from ${mistVolume}", "start").id

  echo 'test Mist with Spark version - ' + sparkVersion
//  def mistId = docker.image("hydrosphere/mist:tests-${sparkVersion}").run(" -l ${mosquittoId}:mosquitto -l ${hdfsId}:hdfs -v ${env.WORKSPACE}:/usr/share/mist", "tests").id
//  sh "docker logs -f ${mistId}"
  
  echo 'remove containers'
//  docker.rm(mosquittoId)
//  docker.rm(mistVolume)
//  docker.rm(hdfsId)
//  docker.rm(mistId)
}
