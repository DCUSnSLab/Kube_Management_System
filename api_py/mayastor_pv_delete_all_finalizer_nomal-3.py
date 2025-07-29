from kubernetes import client, config

def delete_pvs_with_storage_classes(target_storage_class):
    # kubeconfig 로드 (클러스터 내부에서는 config.load_incluster_config() 사용)
    config.load_kube_config()

    v1 = client.CoreV1Api()

    pv_list = v1.list_persistent_volume().items

    for pv in pv_list:
        name = pv.metadata.name
        sc_name = pv.spec.storage_class_name
        finalizers = pv.metadata.finalizers

        if sc_name == target_storage_class and finalizers:
            print(f"[INFO] PV '{name}' has finalizers: {finalizers} → removing...")

            # Finalizer 제거
            pv.metadata.finalizers = []

            # 적용
            try:
                v1.replace_persistent_volume(name, pv)
                print(f"[SUCCESS] Finalizers removed from PV '{name}'")
            except client.exceptions.ApiException as e:
                print(f"[ERROR] Failed to update PV '{name}': {e}")

if __name__ == "__main__":
    target_storage_class = "mayastor-normal-3"
    delete_pvs_with_storage_classes(target_storage_class)

