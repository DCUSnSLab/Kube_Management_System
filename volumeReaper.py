import os
from datetime import datetime, timezone, timedelta

from kubernetes import client, config

from DB_postgresql import initialize_database, save_volume_reap, get_pod_deleted_map

TARGET_NAMESPACE = os.getenv("REAPER_TARGET_NAMESPACE", "swlabpods")
LONGHORN_NAMESPACE = "longhorn-system"
STALE_DAYS = int(os.getenv("REAPER_STALE_DAYS", "90"))
MAX_DELETE_PER_RUN = int(os.getenv("REAPER_MAX_DELETE", "20"))
MAX_DELETE_ERRORS = 5
DELETE_MODE = os.getenv("REAPER_DELETE", "").lower() in ("1", "true", "yes")
PVC_PREFIX = "ssh-"
PROTECTED_KEYWORDS = ("wldnjs269", "marsberry")


def isProtected(pvc_name):
    return any(k in pvc_name for k in PROTECTED_KEYWORDS)


def podNameForPvc(pvc_name):
    return pvc_name[:-4] if pvc_name.endswith("-pvc") else pvc_name


def parseTime(raw):
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def asUtc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def parseSizeGi(quantity):
    if not quantity:
        return None
    q = str(quantity)
    try:
        if q.endswith("Gi"):
            return float(q[:-2])
        if q.endswith("Mi"):
            return float(q[:-2]) / 1024
        if q.endswith("Ti"):
            return float(q[:-2]) * 1024
        return int(q) / 1024 ** 3
    except ValueError:
        return None


def classifyPvcs(pvc_names, sizes, longhorn_map, db_deleted_map, in_use, now):
    """
    회수 후보 선별. 마지막 사용 시각은 Longhorn lastPodRefAt과
    GC DB deleted_at 중 확보되는 신호를 사용(둘 다 있으면 최신값).
    신호가 하나도 없으면 unevaluated로 분리해 삭제 대상에서 제외.
    """
    cutoff = now - timedelta(days=STALE_DAYS)
    candidates, unevaluated = [], []
    for name in pvc_names:
        if name in in_use:
            continue
        lh = longhorn_map.get(name)
        if lh and lh.get("state") != "detached":
            continue
        refs = []
        if lh:
            ref = parseTime(lh.get("last_pod_ref"))
            if ref:
                refs.append(ref)
        db_ref = asUtc(db_deleted_map.get(podNameForPvc(name)))
        if db_ref:
            refs.append(db_ref)
        if not refs:
            unevaluated.append(name)
            continue
        last_ref = max(refs)
        if last_ref >= cutoff:
            continue
        candidates.append({
            "pvc": name,
            "volume": lh.get("volume") if lh else None,
            "size_gi": sizes.get(name),
            "last_pod_ref": last_ref,
        })
    candidates.sort(key=lambda c: c["last_pod_ref"])
    return candidates, unevaluated


class VolumeReaper:
    def __init__(self):
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.core = client.CoreV1Api()
        self.objs = client.CustomObjectsApi()

    def listTargetPvcs(self):
        items = self.core.list_namespaced_persistent_volume_claim(TARGET_NAMESPACE).items
        names, sizes = [], {}
        for p in items:
            name = p.metadata.name
            if not name.startswith(PVC_PREFIX) or isProtected(name):
                continue
            names.append(name)
            sizes[name] = parseSizeGi((p.spec.resources.requests or {}).get("storage"))
        return names, sizes

    def getInUsePvcs(self):
        pods = self.core.list_namespaced_pod(TARGET_NAMESPACE).items
        used = set()
        for p in pods:
            for v in (p.spec.volumes or []):
                if v.persistent_volume_claim:
                    used.add(v.persistent_volume_claim.claim_name)
        return used

    def getLonghornMap(self):
        try:
            items = self.objs.list_namespaced_custom_object(
                group="longhorn.io", version="v1beta2",
                namespace=LONGHORN_NAMESPACE, plural="volumes")["items"]
        except Exception as e:
            print(f"[REAPER][WARN] longhorn unavailable, "
                  f"falling back to k8s/DB signals: {e}")
            return {}
        result = {}
        for v in items:
            ks = (v.get("status") or {}).get("kubernetesStatus") or {}
            if ks.get("namespace") != TARGET_NAMESPACE or not ks.get("pvcName"):
                continue
            result[ks["pvcName"]] = {
                "volume": v["metadata"]["name"],
                "state": (v.get("status") or {}).get("state"),
                "last_pod_ref": ks.get("lastPodRefAt"),
            }
        return result

    def verifyDeletable(self, cand):
        """삭제 직전 재검증: 볼륨 상태(가능 시) 또는 최신 파드 목록으로 미사용 재확인"""
        if cand["volume"]:
            try:
                v = self.objs.get_namespaced_custom_object(
                    group="longhorn.io", version="v1beta2",
                    namespace=LONGHORN_NAMESPACE, plural="volumes", name=cand["volume"])
                return (v.get("status") or {}).get("state") == "detached"
            except Exception as e:
                print(f"[REAPER] verify failed for {cand['pvc']}: {e}")
                return False
        try:
            return cand["pvc"] not in self.getInUsePvcs()
        except Exception as e:
            print(f"[REAPER] verify failed for {cand['pvc']}: {e}")
            return False

    def deleteCandidate(self, cand):
        if not cand["pvc"].startswith(PVC_PREFIX) or isProtected(cand["pvc"]):
            raise RuntimeError(f"protected pvc reached delete path: {cand['pvc']}")
        if not self.verifyDeletable(cand):
            print(f"[REAPER] skip {cand['pvc']}: not verified unused")
            save_volume_reap(cand["pvc"], TARGET_NAMESPACE, cand["size_gi"],
                             cand["last_pod_ref"], "skipped", "not verified unused")
            return False
        self.core.delete_namespaced_persistent_volume_claim(cand["pvc"], TARGET_NAMESPACE)
        save_volume_reap(cand["pvc"], TARGET_NAMESPACE, cand["size_gi"],
                         cand["last_pod_ref"], "deleted", "")
        size = f"{cand['size_gi']:.0f}Gi" if cand["size_gi"] else "?"
        print(f"[REAPER] deleted {cand['pvc']} "
              f"(last_pod_ref={cand['last_pod_ref']:%Y-%m-%d}, {size})")
        return True

    def run(self):
        initialize_database()
        now = datetime.now(timezone.utc)

        pvc_names, sizes = self.listTargetPvcs()
        in_use = self.getInUsePvcs()
        longhorn_map = self.getLonghornMap()
        db_deleted_map = get_pod_deleted_map(TARGET_NAMESPACE)

        candidates, unevaluated = classifyPvcs(
            pvc_names, sizes, longhorn_map, db_deleted_map, in_use, now)

        for pvc in unevaluated:
            print(f"[REAPER] cannot evaluate {pvc}: no usage signal")
            save_volume_reap(pvc, TARGET_NAMESPACE, sizes.get(pvc), None,
                             "skipped", "no usage signal")

        total_gi = sum(c["size_gi"] or 0 for c in candidates)
        mode = "DELETE" if DELETE_MODE else "REPORT-ONLY"
        print(f"[REAPER] mode={mode} stale_days={STALE_DAYS} "
              f"pvcs={len(pvc_names)} candidates={len(candidates)} ({total_gi:.0f}Gi) "
              f"unevaluated={len(unevaluated)} "
              f"signals: longhorn={len(longhorn_map)} db={len(db_deleted_map)}")

        for c in candidates:
            save_volume_reap(c["pvc"], TARGET_NAMESPACE, c["size_gi"],
                             c["last_pod_ref"], "report", "")

        if not DELETE_MODE:
            for c in candidates[:10]:
                print(f"  candidate: {c['pvc']} (last_pod_ref={c['last_pod_ref']:%Y-%m-%d})")
            if len(candidates) > 10:
                print(f"  ... and {len(candidates) - 10} more (volume_reap_log 참조)")
            return

        deleted = errors = 0
        for c in candidates:
            if deleted >= MAX_DELETE_PER_RUN:
                print(f"[REAPER] delete cap reached ({MAX_DELETE_PER_RUN}), stopping")
                break
            try:
                if self.deleteCandidate(c):
                    deleted += 1
            except Exception as e:
                errors += 1
                print(f"[REAPER][ERROR] failed to delete {c['pvc']}: {e}")
                save_volume_reap(c["pvc"], TARGET_NAMESPACE, c["size_gi"],
                                 c["last_pod_ref"], "error", str(e)[:200])
                if errors > MAX_DELETE_ERRORS:
                    print("[REAPER] too many delete errors, aborting")
                    break
        print(f"[REAPER] done: deleted={deleted} errors={errors} "
              f"remaining={len(candidates) - deleted}")


if __name__ == "__main__":
    VolumeReaper().run()
