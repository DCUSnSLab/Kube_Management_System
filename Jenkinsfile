// Kube_Management_System (Garbage Collector) — Jenkins Pipeline
// - main 브랜치만 빌드·푸시·배포, 그 외 브랜치는 Checkout 만 수행
// - 이미지: harbor.cu.ac.kr/kube_management_system/gc
// - 배포 대상: k8s-gc 네임스페이스 Deployment/garbage-collector (deploy/gc-deployment.yaml)

pipeline {
    agent any

    options {
        timeout(time: 20, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    environment {
        REGISTRY   = 'harbor.cu.ac.kr'
        IMAGE      = 'kube_management_system/gc'
        NAMESPACE  = 'k8s-gc'
        DEPLOYMENT = 'garbage-collector'
    }

    stages {

        stage('Checkout') {
            steps { checkout scm }
        }

        stage('환경 결정') {
            when { branch 'main' }
            steps {
                script {
                    // 태그 = BUILD_NUMBER-GIT_SHA. 잡을 재생성해 번호가 1 부터 재시작해도
                    // 커밋 SHA 가 달라 Harbor 의 기존 태그를 덮어쓰지 않는다(불변성 보장).
                    env.GIT_SHA   = sh(script: 'git rev-parse --short=7 HEAD', returnStdout: true).trim()
                    env.IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_SHA}"
                    echo "branch=${env.BRANCH_NAME} image=${REGISTRY}/${IMAGE}:${env.IMAGE_TAG}"
                }
            }
        }

        stage('Build & Push') {
            when { branch 'main' }
            steps {
                script {
                    def img = docker.build("${REGISTRY}/${IMAGE}:${IMAGE_TAG}", ".")
                    docker.withRegistry("https://${REGISTRY}", 'harbor') {
                        img.push()
                        img.push('latest')
                    }
                }
            }
        }

        stage('Deploy') {
            when { branch 'main' }
            steps {
                sh """
                    set -e

                    # RBAC(SA/Role/RoleBinding) + Deployment 보장. 이미 있으면 unchanged.
                    kubectl apply -f deploy/gc-deployment.yaml

                    # 이미지 태그 교체 (latest → BUILD_NUMBER-GIT_SHA) 로 롤아웃 트리거
                    kubectl -n ${NAMESPACE} set image deployment/${DEPLOYMENT} \\
                        ${DEPLOYMENT}=${REGISTRY}/${IMAGE}:${IMAGE_TAG}

                    kubectl -n ${NAMESPACE} rollout status deployment/${DEPLOYMENT} --timeout=3m
                """
            }
        }

    }
}
