pipeline {

    agent {
        docker {
            image 'python:2'
        }
    }

    stages {

        stage('Build') {

            steps {
                // environment {} is executed in node context, and there is no WORKSPACE defined
                with_pypirc(pwd()) {
                    sh 'pip install .'
                }
            }

        }
        stage('Test') {

            steps {
                with_pypirc(pwd()) {
                    // install test dependencies
                    sh 'pip install .[test]'
                    // install test runnor
                    sh 'pip install pytest pytest-cov'
                    // TODO: use --cov-report=xml -> coverage.xml
                    sh(script: 'pytest --pyargs org.bccvl.movelib --junitxml=junit.xml --cov-report=html --cov=org.bccvl.movelib',
                       returnStatus: true)
                }
                // capture test result
                step([
                    $class: 'XUnitBuilder',
                    thresholds: [
                        [$class: 'FailedThreshold', failureThreshold: '0',
                                                    unstableThreshold: '1']
                    ],
                    tools: [
                        [$class: 'JUnitType', deleteOutputFiles: true,
                                              failIfNotNew: true,
                                              pattern: 'junit.xml',
                                              stopProcessingIfError: true]
                    ]
                ])
                // publish html coverage report
                publishHTML(target: [
                    allowMissing: false,
                    alwaysLinkToLastBuild: false,
                    keepAll: true,
                    reportDir: 'htmlcov',
                    reportFiles: 'index.html',
                    reportName: 'Coverage Report'
                ])

            }

        }

        stage('Package') {
            when {
                // branch accepts wildcards as well... e.g. "*/master"
                //branch "master"
                expression { currentBuild.result && currentBuild.result == 'SUCCESS' }
            }
            steps {
                sh 'rm -rf build; rm -rf dist'
                with_pypirc(pwd()) {
                    // Build has to happen in correct folder or setup.py won't find MANIFEST.in file and other files
                    sh 'python setup.py register -r dev sdist bdist_wheel upload -r dev'
                }
            }
        }

    }

    post {
        always {
            echo "This runs always"

            // does this plugin get committer emails by themselves?
            // alternative would be to put get commiter email ourselves, and list of people who need to be notified
            // and put mail(...) step into each appropriate section
            // => would this then send 2 emails? e.g. changed + state email?
            step([
                $class: 'Mailer',
                notifyEveryUnstableBuild: true,
                recipients: 'gerhard.weis@gmail.com ' + emailextrecipients([
                    [$class: 'CulpritsRecipientProvider'],
                    [$class: 'RequesterRecipientProvider']
                ])
            ])
        }
        success {
            echo 'This will run only if successful'
        }
        failure {
            echo 'This will run only if failed'
        }
        unstable {
            echo 'This will run only if the run was marked as unstable'
        }
        changed {
            echo 'This will run only if the state of the Pipeline has changed'
            echo 'For example, the Pipeline was previously failing but is now successful'
        }
    }

}


def with_pypirc(home, Closure body) {
    withEnv(["HOME=${home}"]) {
        configFileProvider([configFile(fileId: 'pypirc', variable: 'PYPIRC'),
                            configFile(fileId: 'pip.conf', variable: 'PIPCONF'),
                            configFile(fileId: 'pydistutils.cfg', variable: 'PYDISTUTILSCFG'),
                            configFile(fileId: 'buildout.cfg', variable: 'BUILDOUTCFG')]) {
            sh 'rm -f ~/.pypirc ~/.pydistutils.cfg ~/.pip ~/.buildout'
            sh 'ln -s "${PYPIRC}" ~/.pypirc'
            sh 'ln -s "${PYDISTUTILSCFG}" ~/.pydistutils.cfg'
            sh 'mkdir ~/.pip'
            sh 'ln -s "${PIPCONF}" ~/.pip/pip.conf'
            sh 'mkdir ~/.buildout'
            sh 'ln -s "${BUILDOUTCFG}" ~/.buildout/default.cfg'
            body()
            sh 'rm -fr ~/.pypirc ~/.pydistutils.cfg ~/.pip ~/.buildout'
        }
    }
}
