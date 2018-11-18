node('docker') {

    try {

        stage('Checkout') {
            // clean git clone, but don't fail in case it doesn't exist yet
            sh(script: 'git clean -x -d -f', returnStatus: true)
            checkout scm
        }

        // start up build container
        // TODO: we should pull image from some local resource
        def img = docker.image('python:2')
        img.inside() {

            withVirtualenv() {

                stage('Build') {
                    sh '. ${VIRTUALENV}/bin/activate; pip install -e .'
                }

                stage('Test') {
                    // install test depenhencios
                    sh '. ${VIRTUALENV}/bin/activate; pip install -e .[test]'
                    // install test runner
                    sh '. ${VIRTUALENV}/bin/activate; pip install pytest pytest-cov'
                    // TODO: use --cov-report=xml -> coverage.xml
                    sh(script: '. ${VIRTUALENV}/bin/activate; pytest -v --junitxml=junit.xml --cov-report=xml --cov=org.bccvl.movelib src',
                       returnStatus: true)

                    // capture test result
                    xunit(
                        thresholds: [
                            failed(failureThreshold: '0', 
                                   unstableThreshold: '1')], 
                        tools: [JUnit(
                            deleteOutputFiles: true, 
                            failIfNotNew: true, 
                            pattern: 'junit.xml', 
                            stopProcessingIfError: true)]
                    )
                    // publish html coverage report
                    step([$class: 'CoberturaPublisher',
                          coberturaReportFile: 'coverage.xml']
                    )

                }

                stage('Package') {

                    if (publishPackage(currentBuild.result, env.BRANCH_NAME)) {

                        sh 'rm -fr build dist'
                        sh '. ${VIRTUALENV}/bin/activate; python setup.py register -r devpi sdist bdist_wheel --universal upload -r devpi'

                    }

                }
            }
        }

    } catch(err) {
        throw err
    } finally {

        sh 'git clean -x -d -f'

        step([
            $class: 'Mailer',
            notifyEveryUnstableBuild: true,
            recipients: 'gerhard.weis@gmail.com ' + emailextrecipients([
                [$class: 'CulpritsRecipientProvider'],
                [$class: 'RequesterRecipientProvider']
            ])
        ])

    }

}
