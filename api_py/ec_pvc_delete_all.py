from kubernetes import client, config

def delete_pvcs_all(namespace, exclude, name):
    config.load_kube_config()  # kubeconfig 로드
    v1 = client.CoreV1Api()    # CoreV1 API 사용

    # 해당 네임스페이스의 PVC 목록 가져오기
    pvc_list = v1.list_namespaced_persistent_volume_claim(namespace)

    for pvc in pvc_list.items:
        pvc_name = pvc.metadata.name
        if pvc_name.startswith(name) and pvc_name not in exclude:
            print(f"Deleting PVC: {pvc_name}")
            v1.delete_namespaced_persistent_volume_claim(pvc_name, namespace)

def main():
    namespace = "everycoding"
    name = ""
    exclude = ["redis-pvc", "ec-postgres-pvc"]  # 삭제 제외할 PVC 목록

    delete_pvcs_all(namespace, exclude, name)

if __name__ == '__main__':
    main()
