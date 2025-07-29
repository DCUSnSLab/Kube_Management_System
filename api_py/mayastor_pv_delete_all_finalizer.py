from kubernetes import client, config

def delete_pvs_with_storage_classes(storage_classes):
    # 클러스터 접속 설정
    config.load_kube_config()  # 클러스터 내부에서 실행할 경우: config.load_incluster_config()

    v1 = client.CoreV1Api()

    # 모든 PV 조회
    pv_list = v1.list_persistent_volume().items

    for pv in pv_list:
        sc_name = pv.spec.storage_class_name
        pv_name = pv.metadata.name

        if sc_name in storage_classes:
            print(f"\nProcessing PV: {pv_name} (StorageClass: {sc_name})")

            # finalizers 제거 (patch)
            try:
                patch_body = {
                    "metadata": {
                        "finalizers": None
                    }
                }
                v1.patch_persistent_volume(name=pv_name, body=patch_body)
                print(f"  ➤ Finalizers removed from PV: {pv_name}")
            except client.exceptions.ApiException as e:
                print(f"  ✖ Failed to patch finalizers for PV {pv_name}: {e}")

            # PV 삭제
            try:
                v1.delete_persistent_volume(name=pv_name)
                print(f"  ➤ Successfully deleted PV: {pv_name}")
            except client.exceptions.ApiException as e:
                print(f"  ✖ Failed to delete PV {pv_name}: {e}")

if __name__ == "__main__":
    target_storage_classes = ["mayastor-normal-3", "mayastor-deep-3"]
    delete_pvs_with_storage_classes(target_storage_classes)

