node {
  currentBuild.result = "SUCCESS"
  try {
    stage('clone project') {
      checkout scm
    }
    stage('build and test') {
      test_mist("1.5.2")
      test_mist("1.6.2")
      test_mist("2.0.0")
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
    def mosquittoId = docker.image('ansi/mosquitto:latest').run().imageName()
    def mistVolume = docker.image("hydrosphere/mist:tests-${sparkVersion}").run("-v /usr/share/mist").imageName()
    def hdfsId = docker.image('hydrosphere/hdfs:latest').run("--volumes-from ${mistVolume}", "start").imageName()

    echo "mos - " + mosquittoId
    echo "mist - " + mistVolume
    echo "hdfs - " + hdfsId
    echo 'test Mist with Spark version - ' + sparkVersion
    //def mistId = docker.image("hydrosphere/mist:tests-${sparkVersion}").run(" -l ${mosquittoId}:mosquitto -l ${hdfsId}:hdfs -v ${env.WORKSPACE}:/usr/share/mist", "tests").id
    //sh "docker logs -f ${mistId}"

    echo 'remove containers'
    docker.rm(mosquittoId)
    docker.rm(mistVolume)
    docker.rm(hdfsId)
    //docker.rm(mistId)
  }
}
