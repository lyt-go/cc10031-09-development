"""序列快照领域服务：维护降采样与归并结果的口径说明版本快照与快照水位。
生成指定口径的序列数据版本快照并写入归档快照数据文件。
归档时封存质量报告与归并窗口结果作为归档依据留存；归档途中可判定新到报告
适用的交接批次；报告更正登记后为质检生成待复核窗口清单并标记已落实区间；
旧快照的封存单与关联合计保持冻结，不受后续更正改写。"""
from __future__ import annotations

import re
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SeriesSnapshotService(BaseModuleService):
    MODULE = "series_snapshot"
    ENTITY = "版本快照"
    OPERATION_TERM = "归并"
    LIST_FIELDS = ["快照编号", "口径说明", "测点范围", "时间范围", "序列条数", "生成方式", "快照水位", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "口径说明", "测点范围"]
    STATUS_ORDER = ["草稿", "待确认", "已发布", "已归档"]
    ACTION_RULES = {"提交确认": "待确认", "发布快照": "已发布", "标记归档": "已归档"}
    # 数据归并操作：生成指定口径降采样与归并结果版本快照写入快照归档数据
    NEGATIVE_ACTIONS = []

    # -- 范围解析与封存依据工具 --

    def _seal_dir(self):
        """封存依据目录：交接登记、封存单、更正登记与待复核清单。"""
        return self._data_dir("archive_seals")

    @staticmethod
    def _snapshot_points(snap: dict[str, Any]) -> list[str]:
        raw = str(snap.get("测点范围") or "")
        return [p.strip() for p in re.split(r"[,，、]", raw) if p.strip()]

    @staticmethod
    def _snapshot_window_ids(snap: dict[str, Any]) -> set[str] | None:
        """快照显式声明的覆盖窗口；未声明时按测点范围匹配。"""
        raw = str(snap.get("覆盖窗口") or "").strip()
        if not raw:
            return None
        return {p.strip() for p in re.split(r"[,，、]", raw) if p.strip()}

    def _candidate_windows(self, snap: dict[str, Any]) -> list[dict[str, Any]]:
        """快照交付范围内已归并（已有结果可交付）的归并窗口。"""
        rows = [r for r in store.rows("rollup_window") if r.get("status") == "已归并"]
        window_ids = self._snapshot_window_ids(snap)
        if window_ids is not None:
            return [r for r in rows if str(r.get("窗口编号", "")) in window_ids]
        points = self._snapshot_points(snap)
        return [r for r in rows if str(r.get("测点编号", "")) in points]

    @staticmethod
    def _report_covers(report: dict[str, Any], window_entry: dict[str, Any]) -> bool:
        """质量报告是否覆盖指定窗口：同测点且（无窗口声明或显式覆盖该窗口）。"""
        if str(report.get("测点编号", "")) != str(window_entry.get("测点编号", "")):
            return False
        covers = str(report.get("覆盖窗口") or "").strip()
        if not covers:
            return True
        return str(window_entry.get("窗口编号", "")) in {
            c.strip() for c in re.split(r"[,，、]", covers) if c.strip()
        }

    def _completed_reports_for(self, window_entry: dict[str, Any]) -> list[dict[str, Any]]:
        """覆盖该窗口且已出报告的质量报告，按登记顺序排列。"""
        rows = [
            r for r in store.rows("series_quality")
            if r.get("status") == "已出报告" and self._report_covers(r, window_entry)
        ]
        return sorted(rows, key=lambda r: int(r.get("id", 0)))

    def _window_interval(self, window_no: str) -> tuple[float, float] | None:
        """窗口结果覆盖的序号区间（从归并结果文件读取）。"""
        records = self._read_jsonl(self._data_dir("rollup_windows") / f"{window_no}_rollup.jsonl")
        if not records:
            return None
        starts, ends = [], []
        for r in records:
            try:
                starts.append(float(r.get("window_start", 0)))
                ends.append(float(r.get("window_end", r.get("window_start", 0))))
            except (ValueError, TypeError):
                continue
        if not starts:
            return None
        return (min(starts), max(ends))

    def _window_cumulative(self, window_no: str) -> float:
        """窗口的关联合计：优先取累计聚合终值，否则按归并结果求和。"""
        rollup_dir = self._data_dir("rollup_windows")
        cum = self._read_jsonl(rollup_dir / f"{window_no}_cumulative.jsonl")
        if cum:
            try:
                return round(float(cum[-1].get("cumulative_value", 0)), 4)
            except (ValueError, TypeError):
                pass
        total = 0.0
        for r in self._read_jsonl(rollup_dir / f"{window_no}_rollup.jsonl"):
            try:
                total += float(r.get("rollup_value", 0))
            except (ValueError, TypeError):
                continue
        return round(total, 4)

    @staticmethod
    def _parse_range(range_text: Any) -> tuple[float, float] | None:
        parts = [p for p in re.split(r"[-~～—]", str(range_text)) if p.strip()]
        if len(parts) < 2:
            return None
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            return None

    def _range_overlaps(self, range_text: Any, interval: tuple[float, float] | None) -> bool:
        if interval is None:
            return False
        parsed = self._parse_range(range_text)
        if parsed is None:
            return True  # 无法解析的区间保守视为相关
        return parsed[0] <= interval[1] and interval[0] <= parsed[1]

    # -- 交接登记与封存状态 --

    def _record_handover(self, handover_id: str, **fields: Any) -> None:
        """追加一条交接事件（进行中/已封存/已驳回），读取时取同号最后一条。"""
        record = {"handover_id": handover_id, **fields}
        self._write_jsonl(self._seal_dir() / "handovers.jsonl", [record])

    def _handovers(self) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for r in self._read_jsonl(self._seal_dir() / "handovers.jsonl"):
            if r.get("handover_id"):
                latest[r["handover_id"]] = r
        return latest

    def _sealed_window_ids(self) -> set[str]:
        """已被某次已封存交接封存的归并窗口编号集合。"""
        sealed: set[str] = set()
        for h in self._handovers().values():
            if h.get("status") == "已封存":
                sealed.update(h.get("windows") or [])
        return sealed

    def _corrections(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self._seal_dir() / "corrections.jsonl")

    def _review_tasks(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self._seal_dir() / "review_tasks.jsonl")

    def _effective_quality(self, window_entry: dict[str, Any]) -> dict[str, Any] | None:
        """窗口当前有效质量判断：最新已出报告为底，被已落实更正覆盖时取更正结论。
        返回 None 表示暂缺质检，窗口不可封存。"""
        reports = self._completed_reports_for(window_entry)
        if not reports:
            return None
        report = reports[-1]
        window_no = str(window_entry.get("窗口编号", ""))
        conclusion = str(report.get("质检结论") or "未判定")
        source = f"报告:{report.get('报告编号')}"
        interval = self._window_interval(window_no)
        for corr in self._corrections():
            if corr.get("status") != "已落实":
                continue
            if str(corr.get("point", "")) != str(window_entry.get("测点编号", "")):
                continue
            covered = corr.get("covered_windows") or []
            if covered and window_no not in covered:
                continue
            if not self._range_overlaps(corr.get("window_range", ""), interval):
                continue
            conclusion = str(corr.get("corrected_conclusion") or conclusion)
            source = f"更正:{corr.get('correction_id')}"
        return {"conclusion": conclusion, "source": source, "report_id": report.get("报告编号")}

    # -- 状态流转拦截：未封存的窗口不能通过更改快照状态被当作已交付 --

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "标记归档":
            snap = store.find(self.MODULE, entry_id)
            if snap is not None:
                sealed = self._sealed_window_ids()
                unsealed = [
                    str(w.get("窗口编号", ""))
                    for w in self._candidate_windows(snap)
                    if str(w.get("窗口编号", "")) not in sealed
                ]
                if unsealed:
                    return None, (
                        f"存在未封存归并窗口（{'、'.join(unsealed)}），"
                        "未封存的窗口不能通过更改快照状态被当作已交付"
                    )
        return super().run_action(entry_id, action)

    # -- 主流程 --

    def create_snapshot(self, snapshot_id: int) -> dict[str, Any]:
        """生成指定口径的降采样与归并结果版本快照，写入快照归档数据文件。"""
        snap = store.find(self.MODULE, snapshot_id)
        if snap is None:
            return {"error": f"版本快照 {snapshot_id} 不存在"}
        self.run_action(snapshot_id, "提交确认")
        caliber = snap.get("口径说明", "标准口径")
        point_range = snap.get("测点范围", "")
        time_range = snap.get("时间范围", "")

        # 扫描归并结果作为快照数据源
        snapshot_data = []
        rollup_dir = self._data_dir("rollup_windows")
        if rollup_dir.exists():
            for f in rollup_dir.glob("*_rollup.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    snapshot_data.append({
                        "snapshot_id": snap["快照编号"],
                        "caliber": caliber,
                        "point": r.get("point", ""),
                        "window_index": r.get("window_start", 0),
                        "rollup_value": r.get("rollup_value", 0),
                        "metric": r.get("metric", ""),
                    # 数据归档输出：将已发布快照归档到冷区存储写入快照归档日志文件
                    })

        ds_dir = self._data_dir("downsampled")
        if ds_dir.exists():
            for f in ds_dir.glob("*_downsampled.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    snapshot_data.append({
                        "snapshot_id": snap["快照编号"],
                        "caliber": caliber,
                        "point": r.get("point", ""),
                        "window_index": r.get("window_index", 0),
                        "aggregated_value": r.get("aggregated_value", 0),
                        "method": r.get("method", ""),
                    })
        # 也包含质检通过的数据
        quality_dir = self._data_dir("quality_reports")
        if quality_dir.exists():
            for f in quality_dir.glob("*_quality.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    snapshot_data.append({
                        "snapshot_id": snap["快照编号"],
                        "caliber": caliber,
                        "quality_check_point": r.get("point", ""),
                        "conclusion": r.get("conclusion", ""),
                        "completeness": r.get("completeness", 0),
                    })

        # 将快照数据写入序列版本快照归档文件
        output_file = self._data_dir("snapshots") / f"{snap['快照编号']}_snapshot.jsonl"
        written = self._write_jsonl(output_file, snapshot_data)
        snap["序列条数"] = str(written)
        snap["生成方式"] = "归并+降采样+质量聚合"
        snap["快照水位"] = str(written)
        snap["生成时间"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        self.run_action(snapshot_id, "发布快照")
        # 登记一次进行中的交接：从发布到归档封存视为同一次交接，
        # 归档途中新到的质量报告可按测点归属判定适用于哪次交接。
        handover_id = f"HO-{snap['快照编号']}"
        if handover_id not in self._handovers():
            self._record_handover(
                handover_id,
                snapshot_id=snapshot_id,
                快照编号=snap["快照编号"],
                points=self._snapshot_points(snap),
                window_scope=sorted(self._snapshot_window_ids(snap) or []),
                status="进行中",
                opened_at=time.time(),
            )
        return {"snapshot_id": snap["快照编号"], "caliber": caliber,
                "records_in_snapshot": written}

    def archive_snapshot(self, snapshot_id: int) -> dict[str, Any]:
        """归档已发布快照：封存质量报告与归并窗口结果作为归档依据后转入冷区。
        范围内存在未封存窗口（暂缺质检）时拒绝归档，交接记为已驳回。"""
        snap = store.find(self.MODULE, snapshot_id)
        if snap is None:
            return {"error": f"版本快照 {snapshot_id} 不存在"}
        if snap.get("status") == "已归档":
            return {"error": f"版本快照 {snap['快照编号']} 已归档，勿重复归档"}
        handover_id = f"HO-{snap['快照编号']}"
        points = self._snapshot_points(snap)
        if handover_id not in self._handovers():
            self._record_handover(
                handover_id, snapshot_id=snapshot_id, 快照编号=snap["快照编号"],
                points=points, status="进行中", opened_at=time.time(),
            )

        # 封存前质检：交付范围内已归并窗口必须有已完成的质量报告作为归档依据
        basis_windows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        unsealed: list[str] = []
        for w in self._candidate_windows(snap):
            quality = self._effective_quality(w)
            if quality is None:
                unsealed.append(str(w.get("窗口编号", "")))
            else:
                basis_windows.append((w, quality))
        if unsealed:
            self._record_handover(
                handover_id, snapshot_id=snapshot_id, 快照编号=snap["快照编号"],
                points=points, status="已驳回",
                reason=f"未封存窗口：{'、'.join(unsealed)}", closed_at=time.time(),
            )
            return {"error": f"存在未封存归并窗口（{'、'.join(unsealed)}），归档被拒绝",
                    "unsealed_windows": unsealed}

        # 封存归档依据：归并窗口结果（含关联合计与有效质量判断）+ 已完成的质量报告
        seal_manifest: list[dict[str, Any]] = []
        report_ids: set[str] = set()
        for w, quality in basis_windows:
            window_no = str(w.get("窗口编号", ""))
            interval = self._window_interval(window_no)
            cumulative = self._window_cumulative(window_no)
            seal_manifest.append({
                "kind": "rollup_window",
                "window_id": window_no,
                "point": w.get("测点编号", ""),
                "window_start": interval[0] if interval else None,
                "window_end": interval[1] if interval else None,
                "cumulative_total": cumulative,
                "quality_conclusion": quality["conclusion"],
                "quality_source": quality["source"],
                "report_id": quality["report_id"],
                "checksum": self._compute_checksum(f"{window_no}|{cumulative}|{quality['conclusion']}"),
                "sealed_at": time.time(),
            })
            if quality["report_id"]:
                report_ids.add(str(quality["report_id"]))
        for report_no in sorted(report_ids):
            report = next(
                (r for r in store.rows("series_quality") if str(r.get("报告编号", "")) == report_no),
                None,
            )
            if report is None:
                continue
            seal_manifest.append({
                "kind": "quality_report",
                "report_id": report_no,
                "point": report.get("测点编号", ""),
                "conclusion": report.get("质检结论", ""),
                "completeness": report.get("完整率", ""),
                "checksum": self._compute_checksum(f"{report_no}|{report.get('质检结论', '')}"),
                "sealed_at": time.time(),
            })
        # 封存单一旦写入不再改写：旧快照须保留其关联合计与封存时的质量判断
        self._write_jsonl(self._seal_dir() / f"{snap['快照编号']}_seal.jsonl", seal_manifest)

        # 交接封存：已完成的质量报告与归并窗口作为归档依据留存
        self._record_handover(
            handover_id, snapshot_id=snapshot_id, 快照编号=snap["快照编号"], points=points,
            status="已封存", sealed_at=time.time(),
            windows=[str(w.get("窗口编号", "")) for w, _ in basis_windows],
            reports=sorted(report_ids),
        )

        # 归档记录到冷区
        snap_file = self._data_dir("snapshots") / f"{snap['快照编号']}_snapshot.jsonl"
        records = self._read_jsonl(snap_file)
        self._write_jsonl(
            self._data_dir("snapshots") / f"{snap['快照编号']}_archived.jsonl",
            [{"snapshot_id": snap["快照编号"], "archived_at": time.time(),
              "record_count": len(records), "cold_path": f"/cold/snapshots/{snap['快照编号']}",
              "sealed_windows": len(basis_windows), "sealed_reports": len(report_ids)}]
        )
        self.run_action(snapshot_id, "标记归档")
        return {"snapshot_id": snap["快照编号"], "archived_records": len(records),
                "sealed_windows": len(basis_windows), "sealed_reports": len(report_ids),
                "handover_id": handover_id, "status": "已归档"}

    def classify_report_handover(self, report_id: Any) -> dict[str, Any]:
        """判断归档途中新到的质量报告适用于哪一次交接。
        按测点归属匹配交接范围，进行中的交接优先于已封存的交接。"""
        report = None
        for r in store.rows("series_quality"):
            if str(r.get("报告编号", "")) == str(report_id):
                report = r
                break
        if report is None and str(report_id).isdigit():
            report = store.find("series_quality", int(report_id))
        if report is None:
            return {"error": f"质量报告 {report_id} 不存在"}
        point = str(report.get("测点编号", ""))
        handovers = [h for h in self._handovers().values() if point in (h.get("points") or [])]
        in_flight = [h for h in handovers if h.get("status") == "进行中"]
        sealed = [h for h in handovers if h.get("status") == "已封存"]
        pool = in_flight or sealed
        if not pool:
            return {"report_id": report.get("报告编号"), "point": point,
                    "handover_id": None, "message": "无适用交接"}
        chosen = sorted(
            pool, key=lambda h: float(h.get("opened_at") or h.get("sealed_at") or 0)
        )[-1]
        return {"report_id": report.get("报告编号"), "point": point,
                "handover_id": chosen.get("handover_id"), "快照编号": chosen.get("快照编号"),
                "handover_status": chosen.get("status"),
                "message": f"报告适用于交接 {chosen.get('handover_id')}（{chosen.get('status')}）"}

    def submit_report_correction(self, report_id: Any, corrected_conclusion: str,
                                 window_range: str, correction_id: str | None = None) -> dict[str, Any]:
        """登记报告更正：标记更正已落实的区间，并为质检生成待复核窗口清单。
        同一更正重复提交时幂等返回，不重复登记已落实的补审工作。"""
        report = None
        for r in store.rows("series_quality"):
            if str(r.get("报告编号", "")) == str(report_id):
                report = r
                break
        if report is None and str(report_id).isdigit():
            report = store.find("series_quality", int(report_id))
        if report is None:
            return {"error": f"质量报告 {report_id} 不存在"}
        report_no = str(report.get("报告编号", ""))
        corr_id = correction_id or f"CORR-{report_no}-{window_range}-{corrected_conclusion}"

        # 幂等：同一报告更正已登记过，直接返回既有落实结果，不再登记补审工作
        existing = [c for c in self._corrections() if c.get("correction_id") == corr_id]
        if existing:
            tasks = [t for t in self._review_tasks() if t.get("correction_id") == corr_id]
            return {"correction_id": corr_id, "duplicate": True,
                    "applied_intervals": existing[-1].get("applied_intervals", []),
                    "review_tasks": len(tasks),
                    "message": "同一报告更正已登记，已落实的补审工作不重复登记"}

        covers = str(report.get("覆盖窗口") or "").strip()
        covered_windows = [c.strip() for c in re.split(r"[,，、]", covers) if c.strip()] if covers else []
        point = str(report.get("测点编号", ""))

        # 标记更正已落实的区间：报告覆盖的已归并窗口中与更正区间重叠的部分
        applied: list[dict[str, Any]] = []
        for w in store.rows("rollup_window"):
            if str(w.get("测点编号", "")) != point or w.get("status") != "已归并":
                continue
            if not self._report_covers(report, w):
                continue
            interval = self._window_interval(str(w.get("窗口编号", "")))
            if interval and self._range_overlaps(window_range, interval):
                applied.append({"window_id": w.get("窗口编号"),
                                "window_start": interval[0], "window_end": interval[1]})
        self._write_jsonl(self._seal_dir() / "corrections.jsonl", [{
            "correction_id": corr_id, "report_id": report_no, "point": point,
            "window_range": window_range, "corrected_conclusion": corrected_conclusion,
            "covered_windows": covered_windows, "applied_intervals": applied,
            "status": "已落实", "received_at": time.time()}])

        # 为质检生成待复核窗口清单：已封存交接中引用过该报告的窗口需要补审
        tasks: list[dict[str, Any]] = []
        seq = len(self._review_tasks())
        for h in self._handovers().values():
            if h.get("status") != "已封存" or report_no not in (h.get("reports") or []):
                continue
            for window_no in h.get("windows") or []:
                seq += 1
                tasks.append({"review_id": f"REV-{seq:04d}", "correction_id": corr_id,
                              "report_id": report_no, "handover_id": h.get("handover_id"),
                              "快照编号": h.get("快照编号"), "window_id": window_no,
                              "point": point, "status": "待复核",
                              "created_at": time.time()})
        if tasks:
            self._write_jsonl(self._seal_dir() / "review_tasks.jsonl", tasks)
        return {"correction_id": corr_id, "duplicate": False,
                "applied_intervals": applied, "review_tasks": len(tasks),
                "message": f"更正已落实，生成 {len(tasks)} 条待复核窗口"}

    def downloadable_snapshots(self, point: str | None = None) -> dict[str, Any]:
        """检查哪些快照可用于后续序列下载，以及各自采用哪份质量判断。
        已归档且封存单留存的快照可下载，采用封存时冻结的质量判断与关联合计。"""
        results: list[dict[str, Any]] = []
        corrected_reports = {str(c.get("report_id", "")) for c in self._corrections()}
        for snap in store.rows(self.MODULE):
            points = self._snapshot_points(snap)
            if point and point not in points:
                continue
            manifest = self._read_jsonl(self._seal_dir() / f"{snap.get('快照编号')}_seal.jsonl")
            if snap.get("status") == "已归档" and manifest:
                windows = [m for m in manifest if m.get("kind") == "rollup_window"]
                judgments = {w["window_id"]: w.get("quality_conclusion", "") for w in windows}
                total = round(sum(float(w.get("cumulative_total") or 0) for w in windows), 4)
                report_ids = {str(m.get("report_id", "")) for m in manifest
                              if m.get("kind") == "quality_report"}
                results.append({
                    "快照编号": snap.get("快照编号"),
                    "downloadable": True,
                    "quality_judgment": judgments,
                    "associated_total": total,
                    "review_pending": bool(report_ids & corrected_reports),
                    "basis": "封存质量判断",
                })
            else:
                results.append({
                    "快照编号": snap.get("快照编号"),
                    "downloadable": False,
                    "reason": "缺少封存依据" if snap.get("status") == "已归档" else "未完成封存归档",
                })
        return {"snapshots": results}
