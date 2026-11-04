// Kube_Management_System (Garbage Collector) — Jenkins Pipeline
// - main    → garbage-collector      (k8s-gc, 삭제 실행, 운영 DB)
// - develop → garbage-collector-dev  (k8s-gc, 관측 전용 GC_DELETE=false, dev DB gc-data-dev)
// - 그 외 브랜치는 Checkout 만 수행
// - 이미지: harbor.cu.ac.kr/kube_management_system/gc  (latest 태그는 main 만 갱신)

pipeline {
    agent any

    options {
        timeout(time: 20, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    environment {
        REGISTRY  = 'harbor.cu.ac.kr'
        IMAGE     = 'kube_management_system/gc'
        NAMESPACE = 'k8s-gc'
    }

    stages {

        stage('Checkout') {
            steps { checkout scm }
        }

        stage('환경 결정') {
            when { anyOf { branch 'main'; branch 'develop' } }
            steps {
                script {
                    env.GIT_SHA = sh(script: 'git rev-parse --short=7 HEAD', returnStdout: true).trim()
                    if (env.BRANCH_NAME == 'main') {
                        env.DEPLOYMENT  = 'garbage-collector'
                        env.MANIFEST    = 'deploy/gc-deployment.yaml'
                        env.IMAGE_TAG   = "${env.BUILD_NUMBER}-${env.GIT_SHA}"
                        env.FLOAT_TAG   = 'latest'
                        env.ENV_LABEL   = '운영'
                    } else {
                        env.DEPLOYMENT  = 'garbage-collector-dev'
                        env.MANIFEST    = 'deploy/gc-deployment-dev.yaml'
                        env.IMAGE_TAG   = "dev-${env.BUILD_NUMBER}-${env.GIT_SHA}"
                        env.FLOAT_TAG   = 'dev-latest'
                        env.ENV_LABEL   = 'dev'
                    }
                    echo "[${env.ENV_LABEL}] branch=${env.BRANCH_NAME} deploy=${env.DEPLOYMENT} image=${REGISTRY}/${IMAGE}:${env.IMAGE_TAG}"
                }
            }
        }

        stage('Build & Push') {
            when { anyOf { branch 'main'; branch 'develop' } }
            steps {
                script {
                    def img = docker.build("${REGISTRY}/${IMAGE}:${IMAGE_TAG}", ".")
                    docker.withRegistry("https://${REGISTRY}", 'harbor') {
                        img.push()
                        img.push(env.FLOAT_TAG)
                    }
                }
            }
        }

        stage('Deploy') {
            when { anyOf { branch 'main'; branch 'develop' } }
            steps {
                sh """
                    set -e

                    kubectl apply -f ${MANIFEST}

                    # 대상 네임스페이스 라벨·어노테이션은 dev/운영 공통 (라벨만으로는 아무 동작도 하지 않음)
                    kubectl apply --server-side --field-manager=kms-gc -f deploy/gc-targets/namespaces.yaml

                    if [ "${BRANCH_NAME}" = "main" ]; then
                        kubectl apply -f deploy/volume-reaper-cronjob.yaml
                        kubectl apply -f deploy/gc-targets/lablab-workspaces-rbac.yaml
                    fi

                    kubectl -n ${NAMESPACE} set image deployment/${DEPLOYMENT} \\
                        ${DEPLOYMENT}=${REGISTRY}/${IMAGE}:${IMAGE_TAG}

                    kubectl -n ${NAMESPACE} rollout status deployment/${DEPLOYMENT} --timeout=3m
                """
            }
        }

    }
}
