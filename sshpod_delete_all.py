from kubernetes import client, config

def delete_pods_all(namespace, pod_name, exclude):
    config.load_kube_config()  #필수 config값 불러옴
    v1 = client.CoreV1Api()  #api

    pod_list = v1.list_namespaced_pod(namespace) #namespace에 속하는 파드 목록

    for pod in pod_list.items:
        if pod.metadata.name.startswith(pod_name) and pod.metadata.name not in exclude:
            print("Deleting pod:", pod.metadata.name)
            v1.delete_namespaced_pod(pod.metadata.name, namespace)

def main():
    namespace = "swlabpods"
    pod_name = "ssh-"
    exclude = ["ssh-wldnjs269", "ssh-marsberry"]

    delete_pods_all(namespace, pod_name, exclude)

if __name__ == '__main__':
    main()