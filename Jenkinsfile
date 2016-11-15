node {
  currentBuild.result = "SUCCESS"
  try {
    stage('clone project') {
      checkout scm
    }
    stage('build and test') {
      parallel ( failFast: false,
        Spark_1_5_2: { test_mist("1.5.2") },
        Spark_1_6_2: { test_mist("1.6.2") },
        Spark_2_0_0: { test_mist("2.0.0") },
      )
    }
  }
  catch (err) {
    currentBuild.result = "FAILURE"
    echo "${err}"
    gitEmail = sh(returnStdout: true, script: "git --no-pager show -s --format='%ae' HEAD").trim()
    mail body: "project build error is here: ${env.BUILD_URL}" ,
        from: 'hydro-support@provectus.com',
        replyTo: 'noreply@provectus.com',
        subject: 'project build failed',
        to: gitEmail
    throw err
  }
}

def test_mist(sparkVersion)
{
  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
    echo 'prepare for Mist with Spark version - ' + sparkVersion  
    def mosquitto = docker.image('ansi/mosquitto:latest').run()
    def mistVolume = docker.image("hydrosphere/mist:tests-${sparkVersion}").run("-v /usr/share/mist")
    def hdfs = docker.image('hydrosphere/hdfs:latest').run("--volumes-from ${mistVolume.id}", "start")

    echo 'Testing Mist with Spark version: ' + sparkVersion
    def mist = docker.image("hydrosphere/mist:tests-${sparkVersion}").run(" --link ${mosquitto.id}:mosquitto --link ${hdfs.id}:hdfs -v ${env.WORKSPACE}:/usr/share/mist", "tests")
    sh "docker logs -f ${mist.id}"

    echo 'remove containers'
    mosquitto.stop()
    mistVolume.stop()
    hdfs.stop()
    mist.stop()
  }
}
