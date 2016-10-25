node
{
  currentBuild.result = "SUCCESS"

  try {
    stage 'clone projet'
      checkout scm
      echo ${env}

    stage 'build and test'
      parallel ( failFast: false,
          Spark_1_5_2: { test_mist("1.5.2") },
          Spark_1_6_2: { test_mist("1.6.2") },
          Spark_2_0_0: { test_mist("2.0.0") },
      )
  }
  catch (err) {

    currentBuild.result = "FAILURE"

    mail body: "project build error is here: ${env.BUILD_URL}" ,
        from: 'hydro-support@provectus.com',
        replyTo: 'noreply@provectus.com',
        subject: 'project build failed',
        to: "${env.GIT_COMMITTER_EMAIL}"

    throw err
  }
}

def test_mist(sparkVersion)
{
  echo 'prepare for Mist with Spark version - ' + sparkVersion  
  def mistVolume = docker.image("hydrosphere/mist:tests-${sparkVersion}").withRun("-v /usr/share/mist")
  def mosquittoId = docker.image('ansi/mosquitto:latest').id
  def hdfsId = docker.image('hydrosphere/hdfs:latest').withRun("--volumes-from ${mistVolume}", "start").id

  echo 'test Mist with Spark version - ' + sparkVersion
  docker.image("hydrosphere/mist:tests-${sparkVersion}").withRun("-l ${mosquittoId}:mosquitto -l ${hdfsId}:hdfs -v ${env.WORKSPACE}:/usr/share/mist", "tests")
}
