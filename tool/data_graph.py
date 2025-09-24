import os, sys
from typing import List, Optional, Tuple, Dict, Union

from PyQt5.QtGui import QGuiApplication

os.environ["QT_API"] = "pyqt5"
# 맨 위쪽, matplotlib 관련 import 전에
import matplotlib
matplotlib.use("QtAgg")   # 반드시 FigureCanvas import 전에!

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# PyQt는 한 가지만 써야 합니다. (PyQt5와 PySide 혼용 금지)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QComboBox, QListWidget, QListWidgetItem, QTabWidget, QMessageBox,
    QFrame, QPushButton, QLineEdit, QScrollArea, QGroupBox, QSizePolicy
)

import numpy as np
import pandas as pd


# 집계에서 제외할 컬럼들
EXCLUDE_COLS = {
    "pod_name", "pod_ordinal", "pod_ordinary",  # 오타 대응 포함
    "comm", "state", "timestamp", "pid"
}
REQUIRED_FILTER_COLS = ["pod_name", "comm", "state"]
GROUP_COL = "cycle_id"  # x축(시간 정규화)은 cycle_id


# ---------------------------
# 공통 유틸
# ---------------------------
def to_float_or_none(txt: str) -> Optional[float]:
    if txt is None:
        return None
    s = str(txt).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def select_numeric_metric_cols(df: pd.DataFrame) -> List[str]:
    numeric_cols = []
    for c in df.columns:
        if c in EXCLUDE_COLS or c == GROUP_COL:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            numeric_cols.append(c)
    return numeric_cols if numeric_cols else ["utime_delta"]


def prepare_df_base(df: pd.DataFrame) -> pd.DataFrame:
    need = set(REQUIRED_FILTER_COLS + [GROUP_COL])
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise KeyError(f"필수 컬럼 누락: {missing} (필요: {sorted(need)})")
    if "timestamp" in df.columns and not np.issubdtype(df["timestamp"].dtype, np.datetime64):
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df.copy()


def pick_ordinal_col(df: pd.DataFrame) -> str:
    if "pod_ordinal" in df.columns:
        return "pod_ordinal"
    if "pod_ordinary" in df.columns:
        return "pod_ordinary"
    df["pod_ordinal"] = pd.Series([None] * len(df))
    return "pod_ordinal"


def init_combo(combo: QComboBox, df: pd.DataFrame, col: str, add_all=True):
    combo.blockSignals(True)
    combo.clear()
    if add_all:
        combo.addItem("All")
    if col in df.columns:
        vals = df[col].dropna().unique().tolist()
        try:
            vals = sorted(vals)
        except Exception:
            pass
        for v in vals:
            combo.addItem(str(v))
    combo.blockSignals(False)


def combo_val(cb: QComboBox) -> Optional[str]:
    t = cb.currentText()
    return None if (t is None or t == "" or t == "All") else t


def apply_filters(df: pd.DataFrame, filters: List[Tuple[str, Optional[str]]]) -> pd.DataFrame:
    out = df
    for col, val in filters:
        if val is None or col not in out.columns:
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            try:
                vnum = float(val)
                out = out[out[col] == vnum]
            except Exception:
                out = out[out[col].astype(str) == val]
        else:
            out = out[out[col].astype(str) == str(val)]
    return out


def aggregate_series(sub: pd.DataFrame, metric: str, how: str) -> pd.Series:
    if GROUP_COL not in sub.columns or metric not in sub.columns:
        return pd.Series(dtype="float64")
    if how == "Sum":
        return sub.groupby(GROUP_COL, as_index=True)[metric].sum().sort_index()
    else:  # "Mean"
        return sub.groupby(GROUP_COL, as_index=True)[metric].mean().sort_index()

def _rebuild_metric_combo(cb_metric: QComboBox, numeric_cols: list[str], prefer: str = "utime_delta"):
    cb_metric.blockSignals(True)
    cur = cb_metric.currentText()
    cb_metric.clear()
    cb_metric.addItems(numeric_cols)
    idx = cb_metric.findText(cur)
    if idx < 0:
        idx = cb_metric.findText(prefer)
    cb_metric.setCurrentIndex(idx if idx >= 0 else 0)
    cb_metric.blockSignals(False)

# ---------------------------
# 템플릿 패널 (좌/우 단독)
# ---------------------------
class TemplatePanel(QWidget):
    """
    - 옵션:
      * shared_filters=True  : 부모가 제공하는 공통 필터만 사용
      * shared_filters=False : 패널 자체적으로 독립 필터 제공
    - 각 패널은 metric 콤보, aggregation 콤보, y axis (min/max) 입력, 그래프 포함
    """
    def __init__(self, title: str, df: pd.DataFrame, numeric_cols: List[str],
                 shared_filters: bool, parent=None):
        super().__init__(parent)
        self.title = title
        self.df = df
        self.numeric_cols = numeric_cols
        self.shared_filters = shared_filters

        self.ordinal_col = pick_ordinal_col(self.df)

        layout = QVBoxLayout(self)

        # 필터 (독립 모드일 때만 표시)
        if not self.shared_filters:
            filt_box = QHBoxLayout()
            # pod_name
            self.cb_pod_name = QComboBox()
            init_combo(self.cb_pod_name, self.df, "pod_name")
            filt_box.addWidget(QLabel("pod_name"))
            filt_box.addWidget(self.cb_pod_name)

            # pod_ordinal
            self.cb_pod_ordinal = QComboBox()
            init_combo(self.cb_pod_ordinal, self.df, self.ordinal_col)
            filt_box.addWidget(QLabel(self.ordinal_col))
            filt_box.addWidget(self.cb_pod_ordinal)

            # comm
            self.cb_comm = QComboBox()
            init_combo(self.cb_comm, self.df, "comm")
            filt_box.addWidget(QLabel("comm"))
            filt_box.addWidget(self.cb_comm)

            # state
            self.cb_state = QComboBox()
            init_combo(self.cb_state, self.df, "state")
            filt_box.addWidget(QLabel("state"))
            filt_box.addWidget(self.cb_state)

            filt_box.addStretch(1)
            layout.addLayout(filt_box)

        # 상단 컨트롤: Metric / Aggregation / Y range
        ctrl = QHBoxLayout()
        ctrl.addWidget(QLabel(f"{title} - Metric"))
        self.cb_metric = QComboBox()
        self.cb_metric.addItems(self.numeric_cols)
        ctrl.addWidget(self.cb_metric)

        ctrl.addSpacing(12)
        ctrl.addWidget(QLabel("Aggregation"))
        self.cb_agg = QComboBox()
        self.cb_agg.addItems(["Sum", "Mean"])
        ctrl.addWidget(self.cb_agg)

        ctrl.addSpacing(12)
        ctrl.addWidget(QLabel("Y min"))
        self.le_ymin = QLineEdit()
        self.le_ymin.setFixedWidth(90)
        ctrl.addWidget(self.le_ymin)
        ctrl.addWidget(QLabel("Y max"))
        self.le_ymax = QLineEdit()
        self.le_ymax.setFixedWidth(90)
        ctrl.addWidget(self.le_ymax)

        ctrl.addStretch(1)
        layout.addLayout(ctrl)

        # 그래프
        self.fig = Figure(figsize=(5, 3))
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        # 콜백: 부모가 할당 (공유필터/독립필터 변경 시 다시 그림)
        self.on_redraw = None

        # 이벤트 연결
        self.cb_metric.currentIndexChanged.connect(self._changed)
        self.cb_agg.currentIndexChanged.connect(self._changed)
        if not self.shared_filters:
            for cb in (self.cb_pod_name, self.cb_pod_ordinal, self.cb_comm, self.cb_state):
                cb.currentIndexChanged.connect(self._changed)
        self.le_ymin.editingFinished.connect(self._changed)
        self.le_ymax.editingFinished.connect(self._changed)

    def _changed(self, *_):
        if self.on_redraw:
            self.on_redraw()

    def set_defaults(self, default_metric: str = "utime_delta", default_agg: str = "Mean"):
        # 신호 일시 차단
        self.cb_metric.blockSignals(True)
        self.cb_agg.blockSignals(True)
        try:
            # metric 기본값
            if self.cb_metric.count() > 0:
                idx = self.cb_metric.findText(default_metric)
                self.cb_metric.setCurrentIndex(idx if idx >= 0 else 0)
            # aggregation 기본값
            idx = self.cb_agg.findText(default_agg)
            if idx >= 0:
                self.cb_agg.setCurrentIndex(idx)
        finally:
            self.cb_metric.blockSignals(False)
            self.cb_agg.blockSignals(False)

    # shared mode일 때 부모가 넘겨주는 필터
    def current_filters(self, shared_filter_values: Optional[List[Tuple[str, Optional[str]]]] = None
                        ) -> List[Tuple[str, Optional[str]]]:
        if self.shared_filters:
            return shared_filter_values or []
        # 독립 필터 모드
        f = [
            ("pod_name", combo_val(self.cb_pod_name)),
            (self.ordinal_col, combo_val(self.cb_pod_ordinal)),
            ("comm", combo_val(self.cb_comm)),
            ("state", combo_val(self.cb_state)),
        ]
        return f

    def current_metric(self) -> Optional[str]:
        return self.cb_metric.currentText() if self.cb_metric.count() > 0 else None

    def current_agg(self) -> str:
        return self.cb_agg.currentText()

    def y_limits(self) -> Tuple[Optional[float], Optional[float]]:
        return to_float_or_none(self.le_ymin.text()), to_float_or_none(self.le_ymax.text())

    def draw_series(self, s: pd.Series):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        if s is not None and len(s) > 0:
            x = s.index.values
            y = s.values
            ax.plot(x, y, marker="o")
            ax.set_xlim(min(x), max(x))
        else:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)

        ymin, ymax = self.y_limits()
        if ymin is not None or ymax is not None:
            ax.set_ylim(bottom=ymin if ymin is not None else None,
                        top=ymax if ymax is not None else None)

        title_metric = self.current_metric() or ""
        ax.set_title(f"[{self.title}] {self.current_agg()} {title_metric} by {GROUP_COL}")
        ax.set_xlabel(GROUP_COL)
        ax.set_ylabel("value")
        ax.grid(True, linestyle="--", alpha=0.4)
        self.canvas.draw()


# ---------------------------
# 탭3: 멀티 시리즈 오버레이
# ---------------------------
# (유틸은 기존 코드 그대로 사용)
# - select_numeric_metric_cols, pick_ordinal_col, init_combo, combo_val,
#   apply_filters, aggregate_series, to_float_or_none
# - SeriesPanel는 아래처럼 약간만 수정: 신호 연결은 부모에서 하고 redraw는 부모가 호출

class SeriesPanel(QWidget):
    def __init__(self, title: str, df: pd.DataFrame, numeric_cols: list[str], datasets: dict[str, pd.DataFrame] = None, parent=None):
        super().__init__(parent)
        self.df = df
        self.numeric_cols = numeric_cols
        self.datasets = datasets or {}
        self.ordinal_col = pick_ordinal_col(self.df)
        self.on_delete = None
        self.on_changed = None

        box = QGroupBox(title); self._box = box
        lay = QVBoxLayout(box)

        top_row = QHBoxLayout()

        # ★ dataset 콤보
        if self.datasets:
            top_row.addWidget(QLabel("dataset"))
            self.cb_dataset = QComboBox()
            self.cb_dataset.addItems(list(self.datasets.keys()))
            self.cb_dataset.setCurrentIndex(0)
            self.cb_dataset.currentIndexChanged.connect(self._on_dataset_changed)
            top_row.addWidget(self.cb_dataset)

        # 필터 영역
        self.cb_pod_name = QComboBox(); init_combo(self.cb_pod_name, df, "pod_name")
        top_row.addWidget(QLabel("pod_name")); top_row.addWidget(self.cb_pod_name)

        self.cb_pod_ordinal = QComboBox(); init_combo(self.cb_pod_ordinal, df, self.ordinal_col)
        top_row.addWidget(QLabel(self.ordinal_col)); top_row.addWidget(self.cb_pod_ordinal)

        self.cb_comm = QComboBox(); init_combo(self.cb_comm, df, "comm")
        top_row.addWidget(QLabel("comm")); top_row.addWidget(self.cb_comm)

        self.cb_state = QComboBox(); init_combo(self.cb_state, df, "state")
        top_row.addWidget(QLabel("state")); top_row.addWidget(self.cb_state)

        # 오른쪽 끝: 삭제 버튼
        top_row.addStretch(1)
        self.btn_delete = QPushButton("삭제")
        self.btn_delete.setFixedWidth(64)
        self.btn_delete.clicked.connect(self._handle_delete)
        top_row.addWidget(self.btn_delete)

        lay.addLayout(top_row)

        # metric/agg 행
        ctrl = QHBoxLayout()
        self.cb_metric = QComboBox(); self.cb_metric.addItems(self.numeric_cols)
        ctrl.addWidget(QLabel("Metric")); ctrl.addWidget(self.cb_metric)
        self.cb_agg = QComboBox(); self.cb_agg.addItems(["Sum", "Mean"])
        ctrl.addWidget(QLabel("Aggregation")); ctrl.addWidget(self.cb_agg)
        ctrl.addStretch(1)
        lay.addLayout(ctrl)

        outer = QVBoxLayout(self)
        outer.addWidget(box)

    def set_title(self, title: str):
        self._box.setTitle(title)

    def _handle_delete(self):
        if callable(self.on_delete):
            self.on_delete(self)  # 부모에 자신을 넘겨서 삭제 요청

    def set_defaults(self, default_metric: str = "utime_delta", default_agg: str = "Mean"):
        # 신호 일시 차단
        self.cb_metric.blockSignals(True)
        self.cb_agg.blockSignals(True)
        try:
            # metric 기본값
            if self.cb_metric.count() > 0:
                idx = self.cb_metric.findText(default_metric)
                self.cb_metric.setCurrentIndex(idx if idx >= 0 else 0)
            # aggregation 기본값
            idx = self.cb_agg.findText(default_agg)
            if idx >= 0:
                self.cb_agg.setCurrentIndex(idx)
        finally:
            self.cb_metric.blockSignals(False)
            self.cb_agg.blockSignals(False)

    def set_dataframe(self, df: pd.DataFrame, numeric_cols: list[str]):
        self.df = df
        self.numeric_cols = numeric_cols
        self.ordinal_col = pick_ordinal_col(self.df)
        init_combo(self.cb_pod_name, self.df, "pod_name")
        init_combo(self.cb_pod_ordinal, self.df, self.ordinal_col)
        init_combo(self.cb_comm, self.df, "comm")
        init_combo(self.cb_state, self.df, "state")
        _rebuild_metric_combo(self.cb_metric, self.numeric_cols, "utime_delta")

    def _on_dataset_changed(self):
        if not self.datasets:
            return
        key = self.cb_dataset.currentText()
        df_new = prepare_df_base(self.datasets[key])
        nc_new = select_numeric_metric_cols(df_new)
        self.set_dataframe(df_new, nc_new)
        self.set_defaults("utime_delta", "Mean")
        # ★ 부모 redraw 대신 콜백 호출
        if callable(self.on_changed):
            self.on_changed()

    def current_filter_values(self):
        """라벨용: 현재 콤보 선택값을 그대로 반환(None이면 All로 표시 예정)"""
        return {
            "pod_name": combo_val(self.cb_pod_name),
            "pod_ordinal": combo_val(self.cb_pod_ordinal),  # or ordinary 내부적으로 이미 처리됨
            "comm": combo_val(self.cb_comm),
            "state": combo_val(self.cb_state),
        }

    def filters(self):
        return [
            ("pod_name", combo_val(self.cb_pod_name)),
            (self.ordinal_col, combo_val(self.cb_pod_ordinal)),
            ("comm", combo_val(self.cb_comm)),
            ("state", combo_val(self.cb_state)),
        ]

    def current_dataset(self) -> str:
        if hasattr(self, "cb_dataset"):
            return self.cb_dataset.currentText()
        return "default"

    def metric(self): return self.cb_metric.currentText() if self.cb_metric.count()>0 else None
    def agg(self):    return self.cb_agg.currentText()

class OverlayTab(QWidget):
    """ 멀티 시리즈 오버레이 탭 (Series 영역 고정, 그래프 영역 가변) """
    def __init__(self, df: pd.DataFrame, numeric_cols: list[str], datasets: dict[str, pd.DataFrame] = None, parent=None):
        super().__init__(parent)
        self.df = df
        self.numeric_cols = numeric_cols
        self.datasets = datasets or {}
        self.series_panels = []
        self._ready = False
        self._child_windows = []

        root = QVBoxLayout(self)

        # 상단: 버튼 + Y범위
        top = QHBoxLayout()
        self.btn_add = QPushButton("+ Add series")
        self.btn_add.clicked.connect(self._add_series_and_draw)
        top.addWidget(self.btn_add)

        # 새창 버튼 추가
        self.btn_popup = QPushButton("Open in new window")
        self.btn_popup.clicked.connect(self._open_detached_window)
        top.addWidget(self.btn_popup)

        top.addSpacing(12)
        top.addWidget(QLabel("Y min"))
        self.le_ymin = QLineEdit(); self.le_ymin.setFixedWidth(90)
        top.addWidget(self.le_ymin)
        top.addWidget(QLabel("Y max"))
        self.le_ymax = QLineEdit(); self.le_ymax.setFixedWidth(90)
        top.addWidget(self.le_ymax)
        top.addStretch(1)
        root.addLayout(top)

        # --- Series 영역(스크롤) : 높이 고정 200 ---
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setFixedHeight(200)                     # ★ 고정 높이
        self.scroll.setSizePolicy(QSizePolicy.Expanding,    # 가로는 확장
                                  QSizePolicy.Fixed)        # 세로는 고정

        self.inner = QWidget()
        self.inner_layout = QVBoxLayout(self.inner)
        self.inner_layout.setAlignment(Qt.AlignTop)
        self.scroll.setWidget(self.inner)

        # --- 그래프 영역 : 창 크기에 따라 확장 ---
        self.fig = Figure(figsize=(6, 3.5))
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)  # ★ 확장
        # stretch 설정: scroll(0), canvas(1)
        root.addWidget(self.scroll, stretch=0)
        root.addWidget(self.canvas, stretch=1)

        # 입력 이벤트(준비 완료 후 반영)
        self.le_ymin.editingFinished.connect(self._safe_redraw)
        self.le_ymax.editingFinished.connect(self._safe_redraw)

        # 기본 시리즈는 지연 초기화로 추가
        QTimer.singleShot(0, self._post_init)

    def _open_detached_window(self):
        win = OverlayWindow(self.df, self.numeric_cols, datasets=self.datasets)
        self._child_windows.append(win)
        win.show()
        center_on_primary(win)

    def _post_init(self):
        # 초기 기본 2개 시리즈 추가 (이 시점에서도 redraw는 마지막에 한 번)
        self._add_series(connect_signals=True, do_redraw=False)
        self._add_series(connect_signals=True, do_redraw=False)
        self._ready = True
        self.redraw()

    def _safe_redraw(self):
        if self._ready:
            self.redraw()

    def _wire_panel_signals(self, panel: SeriesPanel):
        # 변경: 삭제 콜백 연결
        panel.on_delete = self.remove_series
        panel.on_changed = self._safe_redraw

        # 기존 필터/메트릭 변경 시그널
        panel.cb_pod_name.currentIndexChanged.connect(self._safe_redraw)
        panel.cb_pod_ordinal.currentIndexChanged.connect(self._safe_redraw)
        panel.cb_comm.currentIndexChanged.connect(self._safe_redraw)
        panel.cb_state.currentIndexChanged.connect(self._safe_redraw)
        panel.cb_metric.currentIndexChanged.connect(self._safe_redraw)
        panel.cb_agg.currentIndexChanged.connect(self._safe_redraw)

        if hasattr(panel, "cb_dataset"):
            panel.cb_dataset.currentIndexChanged.connect(self._safe_redraw)

    def _add_series(self, connect_signals=True, do_redraw=True):
        idx = len(self.series_panels) + 1
        panel = SeriesPanel(f"Series {idx}", self.df, self.numeric_cols, datasets=self.datasets)
        panel.set_defaults("utime_delta", "Mean")
        if connect_signals:
            self._wire_panel_signals(panel)

        self.series_panels.append(panel)
        self.inner_layout.addWidget(panel)

        if do_redraw and self._ready:
            self.redraw()

    def remove_series(self, panel: SeriesPanel):
        """SeriesPanel '삭제' 버튼 콜백에서 호출됨"""
        # 1) 목록에서 제거
        try:
            self.series_panels.remove(panel)
        except ValueError:
            pass

        # 2) 레이아웃에서 제거 후 위젯 삭제
        self.inner_layout.removeWidget(panel)
        panel.setParent(None)
        panel.deleteLater()

        # 3) 남은 패널들 제목 재번호
        for i, p in enumerate(self.series_panels, start=1):
            p.set_title(f"Series {i}")

        # 4) 다시 그리기
        if self._ready:
            self.redraw()

    def _add_series_and_draw(self):
        self._add_series(connect_signals=True, do_redraw=True)

    @staticmethod
    def _fmt_val(v):
        """None -> 'All' 로 치환"""
        return "All" if v is None else str(v)

    def _series_label(self, idx: int, sp: SeriesPanel, metric: str) -> str:
        fv = sp.current_filter_values()
        ds = sp.current_dataset()  # ★ dataset 이름 추가
        return (
            f"S{idx}: {sp.agg()} {metric} | "
            f"dataset={ds}, "  # ★ 추가된 부분
            f"pod_name={self._fmt_val(fv.get('pod_name'))}, "
            f"pod_ordinal={self._fmt_val(fv.get('pod_ordinal'))}, "
            f"comm={self._fmt_val(fv.get('comm'))}, "
            f"state={self._fmt_val(fv.get('state'))}"
        )

    def redraw(self):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        any_data = False

        for i, sp in enumerate(self.series_panels, start=1):
            metric = sp.metric()
            if not metric:
                continue

            df_i = sp.df
            sub = apply_filters(df_i, sp.filters())
            s = aggregate_series(sub, metric, sp.agg())
            if s is None or len(s) == 0:
                continue

            # ★ delta 값 → 맨 처음 1개 제외
            if metric.endswith("_delta"):
                s = s.iloc[1:]

            # ★ cpu_rate → 맨 처음 2개 제외
            if metric == "cpu_rate":
                s = s.iloc[3:]

            if len(s) == 0:
                continue

            any_data = True
            label = self._series_label(i, sp, metric)
            ax.plot(s.index.values, s.values, marker="o", label=label)

        if not any_data:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        else:
            ax.legend(loc="best")

        ymin = to_float_or_none(self.le_ymin.text())
        ymax = to_float_or_none(self.le_ymax.text())
        if ymin is not None or ymax is not None:
            ax.set_ylim(bottom=ymin if ymin is not None else None,
                        top=ymax if ymax is not None else None)

        ax.set_title("Overlay: multiple series by cycle_id")
        ax.set_xlabel("cycle_id")
        ax.set_ylabel("value")
        ax.grid(True, linestyle="--", alpha=0.4)
        self.canvas.draw()

class OverlayWindow(QWidget):
    def __init__(self, df, numeric_cols, datasets=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Overlay (Detached)")
        lay = QVBoxLayout(self)
        self.overlay = OverlayTab(df, numeric_cols, datasets=datasets)  # ★
        lay.addWidget(self.overlay)
        self.resize(900, 600)

class CommStateGrid(QWidget):
    """
    지정 comm(정확 일치, strip+lower) 각각에 대해
    선택 metric을 state(running/sleep)로 분리하여 cycle_id별 시계열로 그리는 그리드.

    추가 타일:
      [0] active_* (avg)  — comm이 'active'로 시작하는 모든 행의 평균
      [1] bg_* (avg)      — comm이 'bg'     로 시작하는 모든 행의 평균

    - 상단 컨트롤: Metric, Aggregation(Sum/Mean: 개별 comm에만 적용), Y-min/max
    - 창 크기 변화에 따라 열 수 동적 조절
    """
    def __init__(self, df: pd.DataFrame, comm_list: list[str], numeric_cols: list[str], parent=None):
        super().__init__(parent)
        self.df = prepare_df_base(df).copy()
        if "comm" not in self.df.columns:
            raise KeyError("CommStateGrid: 데이터에 'comm' 컬럼이 없습니다.")
        if GROUP_COL not in self.df.columns:
            raise KeyError(f"CommStateGrid: 데이터에 '{GROUP_COL}' 컬럼이 없습니다.")

        # 정규화 컬럼
        self.df["__comm_norm__"] = self.df["comm"].astype(str).str.strip().str.lower()
        self.comm_keys = [c.strip().lower() for c in comm_list]
        self.prefix_avgs = [("active", "active_* (avg)"), ("bg", "bg_* (avg)")]

        # 상단 컨트롤
        top = QHBoxLayout()
        top.addWidget(QLabel("Metric"))
        self.cb_metric = QComboBox(); self.cb_metric.addItems(numeric_cols)
        if "utime_delta" in numeric_cols: self.cb_metric.setCurrentText("utime_delta")
        top.addWidget(self.cb_metric)

        top.addSpacing(12)
        top.addWidget(QLabel("Aggregation"))
        self.cb_agg = QComboBox(); self.cb_agg.addItems(["Sum", "Mean"]); self.cb_agg.setCurrentText("Mean")
        top.addWidget(self.cb_agg)

        top.addSpacing(12)
        top.addWidget(QLabel("Y min")); self.le_ymin = QLineEdit(); self.le_ymin.setFixedWidth(90); top.addWidget(self.le_ymin)
        top.addWidget(QLabel("Y max")); self.le_ymax = QLineEdit(); self.le_ymax.setFixedWidth(90); top.addWidget(self.le_ymax)
        top.addStretch(1)

        lay = QVBoxLayout(self)
        lay.addLayout(top)

        self.fig = Figure(figsize=(12, 6))
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        lay.addWidget(self.canvas, stretch=1)

        # 시그널
        self.cb_metric.currentIndexChanged.connect(self._safe_redraw)
        self.cb_agg.currentIndexChanged.connect(self._safe_redraw)
        self.le_ymin.editingFinished.connect(self._safe_redraw)
        self.le_ymax.editingFinished.connect(self._safe_redraw)

        self._ready = True
        self.redraw()

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._safe_redraw()

    @staticmethod
    def _norm_state(x: str) -> str:
        if x is None: return ""
        s = str(x).strip().lower()
        if s == "r" or "run" in s:   return "running"
        if s == "s" or "sleep" in s: return "sleep"
        return s

    def _agg_name(self) -> str:
        return self.cb_agg.currentText()

    def _metric_name(self) -> Optional[str]:
        return self.cb_metric.currentText() if self.cb_metric.count() > 0 else None

    def _y_limits(self) -> tuple[Optional[float], Optional[float]]:
        def f(x):
            try:
                x = x.strip(); return None if x == "" else float(x)
            except Exception:
                return None
        return f(self.le_ymin.text()), f(self.le_ymax.text())

    def _series_metric_exact(self, key: str, metric: str, how: str) -> tuple[pd.Series, pd.Series]:
        d = self.df[self.df["__comm_norm__"] == key].copy()
        if d.empty or metric not in d.columns: return pd.Series(dtype="float64"), pd.Series(dtype="float64")
        d["state_norm"] = d["state"].map(self._norm_state)
        gb = d.groupby([GROUP_COL, "state_norm"], as_index=True)[metric]
        pv = (gb.sum() if how == "Sum" else gb.mean()).unstack(fill_value=0).sort_index()
        s_run = pv["running"] if "running" in pv.columns else pd.Series(0, index=pv.index)
        s_slp = pv["sleep"]   if "sleep"   in pv.columns else pd.Series(0, index=pv.index)
        return s_run, s_slp

    def _series_metric_prefix_avg(self, prefix: str, metric: str) -> tuple[pd.Series, pd.Series]:
        """
        prefix로 시작하는 모든 comm을 묶어 '평균(Mean)'으로 집계.
        (요구사항 명시: 항상 평균)
        """
        d = self.df[self.df["__comm_norm__"].str.startswith(prefix)].copy()
        if d.empty or metric not in d.columns: return pd.Series(dtype="float64"), pd.Series(dtype="float64")
        d["state_norm"] = d["state"].map(self._norm_state)
        # 우선 comm 단위로 cycle_id×state 평균을 구한 뒤, 그 평균들을 다시 평균
        gb = d.groupby([ "__comm_norm__", GROUP_COL, "state_norm"], as_index=False)[metric].mean()
        pv = (gb.pivot_table(index=GROUP_COL, columns="state_norm", values=metric, aggfunc="mean")
                .fillna(0).sort_index())
        s_run = pv["running"] if "running" in pv.columns else pd.Series(0, index=pv.index)
        s_slp = pv["sleep"]   if "sleep"   in pv.columns else pd.Series(0, index=pv.index)
        return s_run, s_slp

    def _calc_cols(self, min_tile_px: int = 340, max_cols: int = 5) -> int:
        w = max(1, self.width())
        return max(1, min(max_cols, w // min_tile_px))

    def _safe_redraw(self, *_):
        if getattr(self, "_ready", False): self.redraw()

    def _plot_pair(self, ax, s_run: pd.Series, s_slp: pd.Series, ylabel: str):
        if len(s_run)==0 and len(s_slp)==0:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            return
        xmins, xmaxs = [], []
        if len(s_run)>0:
            ax.plot(s_run.index.values, s_run.values, marker="o", label="running")
            xmins.append(s_run.index.min()); xmaxs.append(s_run.index.max())
        if len(s_slp)>0:
            ax.plot(s_slp.index.values, s_slp.values, marker="o", label="sleep")
            xmins.append(s_slp.index.min()); xmaxs.append(s_slp.index.max())
        if xmins and xmaxs: ax.set_xlim(min(xmins), max(xmaxs))
        ax.set_xlabel(GROUP_COL); ax.set_ylabel(ylabel); ax.grid(True, linestyle="--", alpha=0.4)

    def redraw(self):
        metric = self._metric_name()
        if not metric: return

        # 타일 순서: [prefix 평균 2개] + [개별 comm 12개]
        tiles = [("prefix", pfx, label) for pfx, label in self.prefix_avgs] + \
                [("exact", key, key) for key in self.comm_keys]

        n = len(tiles)
        cols = max(1, min(self._calc_cols(), n))
        rows = (n + cols - 1) // cols

        self.fig.clear()
        axes = self.fig.subplots(rows, cols, squeeze=False)
        first_legend_ax = None

        for i, (t, key, title) in enumerate(tiles):
            r, c = divmod(i, cols)
            ax = axes[r][c]

            if t == "prefix":
                s_run, s_slp = self._series_metric_prefix_avg(key, metric)   # 항상 평균
                ylab = f"{metric} (Mean)"
            else:
                s_run, s_slp = self._series_metric_exact(key, metric, self._agg_name())
                ylab = f"{metric} ({self._agg_name()})"

            self._plot_pair(ax, s_run, s_slp, ylab)
            ax.set_title(title)
            if first_legend_ax is None: first_legend_ax = ax

        # 남는 축 숨김
        for j in range(n, rows*cols):
            r, c = divmod(j, cols); axes[r][c].axis("off")

        # 공통 Y-limit
        ymin, ymax = self._y_limits()
        if ymin is not None or ymax is not None:
            for rr in range(rows):
                for cc in range(cols):
                    axes[rr][cc].set_ylim(bottom=ymin if ymin is not None else None,
                                          top=ymax if ymax is not None else None)

        if first_legend_ax is not None:
            first_legend_ax.legend(loc="best")

        self.fig.suptitle(
            f"Comm × State by {GROUP_COL} — {metric}",
            fontsize=11, y=0.98
        )
        self.fig.tight_layout(rect=[0, 0, 1, 0.96])
        self.canvas.draw()


# ---------------------------
# 메인 윈도우 (탭 구성)
# ---------------------------
class CyclePlotterApp(QWidget):
    """
    Tab1: 공통 필터 + 좌/우 템플릿(각 템플릿 metric/agg만 독립)
    Tab2: 좌/우 템플릿 독립 필터 + metric/agg
    Tab3: 멀티 시리즈 오버레이(Series 추가 가능)
    모든 그래프에 Y축 min/max 설정 제공
    """
    def __init__(self, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cycle-based Usage Plotter (3 Tabs)")
        self.resize(1500, 900)

        # --- 어댑터: DataFrame 또는 Dict[str, DataFrame] 모두 수용 ---
        if isinstance(data, dict):
            if not data:
                raise ValueError("datasets dict가 비어 있습니다.")
            # 각 DF 정규화
            self.datasets: Dict[str, pd.DataFrame] = {
                str(k): prepare_df_base(v) for k, v in data.items()
            }
        else:
            # 단일 DF를 dict로 감싸서 처리(하위호환)
            self.datasets = {"default": prepare_df_base(data)}
        #self.numeric_cols = select_numeric_metric_cols(self.df)

        self.current_key = next(iter(self.datasets.keys()))
        self.df = self.datasets[self.current_key]
        self.numeric_cols = select_numeric_metric_cols(self.df)

        root = QVBoxLayout(self)
        self.tabs = QTabWidget()
        root.addWidget(self.tabs)

        # ---------- Tab 1 ----------
        self.tab3 = OverlayTab(self.df, self.numeric_cols, datasets=self.datasets)
        self.tabs.addTab(self.tab3, "Tab 1: Overlay (multi-series)")

        # # ---------- Tab 2 ----------
        # tab1 = QWidget(); l1 = QVBoxLayout(tab1)
        #
        # # 공통 필터
        # filt_row = QHBoxLayout()
        # self.cb1_pod_name = QComboBox(); init_combo(self.cb1_pod_name, self.df, "pod_name")
        # self.cb1_ord_col = pick_ordinal_col(self.df)
        # self.cb1_pod_ordinal = QComboBox(); init_combo(self.cb1_pod_ordinal, self.df, self.cb1_ord_col)
        # self.cb1_comm = QComboBox(); init_combo(self.cb1_comm, self.df, "comm")
        # self.cb1_state = QComboBox(); init_combo(self.cb1_state, self.df, "state")
        # for lab, w in [("pod_name", self.cb1_pod_name), (self.cb1_ord_col, self.cb1_pod_ordinal),
        #                ("comm", self.cb1_comm), ("state", self.cb1_state)]:
        #     filt_row.addWidget(QLabel(lab)); filt_row.addWidget(w)
        # filt_row.addStretch(1)
        # l1.addLayout(filt_row)
        #
        # # 좌우 패널
        # panels_row1 = QHBoxLayout()
        # self.panel1_a = TemplatePanel("Template A", self.df, self.numeric_cols, shared_filters=True)
        # self.panel1_b = TemplatePanel("Template B", self.df, self.numeric_cols, shared_filters=True)
        # self.panel1_a.set_defaults("utime_delta", "Mean")
        # self.panel1_b.set_defaults("utime_delta", "Mean")
        # self.panel1_a.on_redraw = self.refresh_tab1
        # self.panel1_b.on_redraw = self.refresh_tab1
        #
        # sep1 = QFrame(); sep1.setFrameShape(QFrame.VLine); sep1.setFrameShadow(QFrame.Sunken)
        # panels_row1.addWidget(self.panel1_a, stretch=1)
        # panels_row1.addWidget(sep1)
        # panels_row1.addWidget(self.panel1_b, stretch=1)
        # l1.addLayout(panels_row1)
        #
        # # 필터 시그널
        # for cb in (self.cb1_pod_name, self.cb1_pod_ordinal, self.cb1_comm, self.cb1_state):
        #     cb.currentIndexChanged.connect(self.refresh_tab1)
        #
        # self.tabs.addTab(tab1, "Tab 2: Shared filters")
        #
        # # ---------- Tab 3 ----------
        # tab2 = QWidget(); l2 = QVBoxLayout(tab2)
        # panels_row2 = QHBoxLayout()
        # self.panel2_a = TemplatePanel("Template A", self.df, self.numeric_cols, shared_filters=False)
        # self.panel2_b = TemplatePanel("Template B", self.df, self.numeric_cols, shared_filters=False)
        # self.panel2_a.set_defaults("utime_delta", "Mean")
        # self.panel2_b.set_defaults("utime_delta", "Mean")
        # self.panel2_a.on_redraw = self.refresh_tab2
        # self.panel2_b.on_redraw = self.refresh_tab2
        #
        # sep2 = QFrame(); sep2.setFrameShape(QFrame.VLine); sep2.setFrameShadow(QFrame.Sunken)
        # panels_row2.addWidget(self.panel2_a, stretch=1)
        # panels_row2.addWidget(sep2)
        # panels_row2.addWidget(self.panel2_b, stretch=1)
        # l2.addLayout(panels_row2)
        #
        # self.tabs.addTab(tab2, "Tab 3: Independent filters per template")
        #
        #
        #
        # # 초기 렌더
        # self.refresh_tab1()
        # self.refresh_tab2()3

        # ---------- Tab 2 (NEW): Comm x State grid ----------
        comm_exact_list = [
            "active_burst", "active_cpu_intensive", "active_io_intensive",
            "active_memory_intensive", "active_multithreaded", "active_resource_intensive",
            "bg_cpu_worker", "bg_memory_cache", "bg_network_service",
            "running_continuous", "running_event_loop", "running_task_queue",
            "inactive_idle", "inactive_sleeping", "inactive_waiting"
        ]

        self.tab_cs = CommStateGrid(
            self.df,
            comm_list=comm_exact_list,
            numeric_cols=self.numeric_cols,  # ← metric 후보 전달!
        )
        # self.tabs.addTab(self.tab_cs, "Tab 2: Comm-State grid")
        #
        # self.tab3.redraw()
        tab2 = QWidget()
        l2 = QVBoxLayout(tab2)
        # 실험 선택 콤보
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("Experiment"))
        self.cb_exp = QComboBox()
        self.cb_exp.setMinimumWidth(120)
        self.cb_exp.addItem("All")  # 전체 합본
        for key in self.datasets.keys():
            self.cb_exp.addItem(key)
        self.cb_exp.currentIndexChanged.connect(self._switch_experiment_tab2)
        top_row.addWidget(self.cb_exp)
        top_row.addStretch(1)
        l2.addLayout(top_row)
        self.tab2_grid = CommStateGrid(self.df, comm_list=comm_exact_list, numeric_cols=self.numeric_cols)
        l2.addWidget(self.tab2_grid)
        self.tabs.addTab(tab2, "Tab 2: Comm-State grid")

        # Tab1 redraw
        self.tab3.redraw()

    # -------- Tab1 --------
    def _tab1_shared_filters(self) -> List[Tuple[str, Optional[str]]]:
        return [
            ("pod_name", combo_val(self.cb1_pod_name)),
            (self.cb1_ord_col, combo_val(self.cb1_pod_ordinal)),
            ("comm", combo_val(self.cb1_comm)),
            ("state", combo_val(self.cb1_state)),
        ]

    def refresh_tab1(self, *_):
        shared = self._tab1_shared_filters()

        # Panel A
        metric_a = self.panel1_a.current_metric()
        if metric_a:
            sub_a = apply_filters(self.df, shared)
            sA = aggregate_series(sub_a, metric_a, self.panel1_a.current_agg())
        else:
            sA = pd.Series(dtype="float64")
        self.panel1_a.draw_series(sA)

        # Panel B
        metric_b = self.panel1_b.current_metric()
        if metric_b:
            sub_b = apply_filters(self.df, shared)
            sB = aggregate_series(sub_b, metric_b, self.panel1_b.current_agg())
        else:
            sB = pd.Series(dtype="float64")
        self.panel1_b.draw_series(sB)

    # -------- Tab2 --------
    def refresh_tab2(self, *_):
        # Panel A
        filt_a = self.panel2_a.current_filters()
        metric_a = self.panel2_a.current_metric()
        if metric_a:
            sub_a = apply_filters(self.df, filt_a)
            sA = aggregate_series(sub_a, metric_a, self.panel2_a.current_agg())
        else:
            sA = pd.Series(dtype="float64")
        self.panel2_a.draw_series(sA)

        # Panel B
        filt_b = self.panel2_b.current_filters()
        metric_b = self.panel2_b.current_metric()
        if metric_b:
            sub_b = apply_filters(self.df, filt_b)
            sB = aggregate_series(sub_b, metric_b, self.panel2_b.current_agg())
        else:
            sB = pd.Series(dtype="float64")
        self.panel2_b.draw_series(sB)

    def _switch_experiment_tab2(self):
        """탭2 실험 선택 콤보박스 이벤트"""
        key = self.cb_exp.currentText()
        if key == "All":
            # 모든 DF를 concat
            df_new = pd.concat(self.datasets.values(), ignore_index=True)
        else:
            df_new = self.datasets[key]

        df_new = prepare_df_base(df_new)
        numeric_cols = select_numeric_metric_cols(df_new)

        # 기존 grid 위젯 교체
        parent_layout = self.tab2_grid.parent().layout()
        parent_layout.removeWidget(self.tab2_grid)
        self.tab2_grid.setParent(None)
        # self.tab2_grid.deleteLater()

        self.tab2_grid = CommStateGrid(df_new, comm_list=self.comm_exact_list, numeric_cols=numeric_cols)
        parent_layout.addWidget(self.tab2_grid)

# ----------------------------
# main: 외부에서 DataFrame을 주입해서 실행
# ----------------------------
def center_on_primary(win):
    """메인 모니터(Primary) 가용 영역의 정중앙에 윈도우를 위치시킵니다."""
    # screen = QGuiApplication.primaryScreen()
    screens = QGuiApplication.screens()
    if not screens:
        return

    # 범위 초과 방지
    screennum = 1  # 띄울 모니터 번호
    idx = max(0, min(screennum, len(screens) - 1))
    screen = screens[idx]

    geo = screen.availableGeometry()     # 작업 표시줄 제외 영역
    # frameGeometry는 show() 이후에 제대로 계산됨
    fg = win.frameGeometry()
    fg.moveCenter(geo.center())
    win.move(fg.topLeft())

def main(df):
    # 이미 QApplication이 있으면 재사용
    app = QApplication.instance()
    if app is None:
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
        app = QApplication(sys.argv)

    w = CyclePlotterApp(df)   # 당신이 만든 최상위 위젯
    w.resize(1280, 720)
    w.show()
    center_on_primary(w)

    sys.exit(app.exec_())