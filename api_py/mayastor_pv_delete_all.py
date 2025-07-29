from kubernetes import client, config

def delete_pvs_with_storage_classes(storage_classes):
    # kubeconfig 로드 (클러스터 내부에서는 config.load_incluster_config() 사용)
    config.load_kube_config()

    v1 = client.CoreV1Api()

    # 모든 PersistentVolume 목록 가져오기
    pv_list = v1.list_persistent_volume().items

    for pv in pv_list:
        sc_name = pv.spec.storage_class_name
        pv_name = pv.metadata.name

        if sc_name in storage_classes:
            print(f"Deleting PV: {pv_name} (StorageClass: {sc_name})")
            try:
                v1.delete_persistent_volume(name=pv_name)
                print(f"Successfully deleted PV: {pv_name}")
            except client.exceptions.ApiException as e:
                print(f"Error deleting PV {pv_name}: {e}")

if __name__ == "__main__":
    target_storage_classes = ["mayastor-normal-3", "mayastor-deep-3"]
    delete_pvs_with_storage_classes(target_storage_classes)

