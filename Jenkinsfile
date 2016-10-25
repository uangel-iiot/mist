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
          Spark_2_0_0: { test_mist("2.0.0") }
      )
  }
  catch (err) {

    currentBuild.result = "FAILURE"

    mail body: "project build error is here: ${env.BUILD_URL}" ,
        from: 'hydro-support@provectus.com',
        subject: 'project build failed',
        to: '${env.GIT_AUTHOR_EMAIL}'

    throw err
  }
}


def test_mist(sparkVersion)
{
  echo 'build ' + sparkVersion
  echo 'prepare ' + sparkVersion
  echo 'test ' + sparkVersion
}
