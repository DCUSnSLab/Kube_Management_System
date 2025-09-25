# test_cycle_plotter_min_fixed.py
import os, sys

# 0) 환경 강제: Qt는 PyQt5, mpl은 QtAgg 사용
os.environ["QT_API"] = "pyqt5"
import matplotlib
matplotlib.use("QtAgg")  # <- 어떤 matplotlib import보다 먼저!

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

import pandas as pd
import numpy as np


def ensure_cycle_id(df, cycle_size=100):
    if "cycle_id" not in df.columns:
        df = df.reset_index(drop=True).copy()
        df["cycle_id"] = (df.index // cycle_size).astype("int64")
    return df


class CyclePlotterApp(QWidget):
    def __init__(self, df):
        super().__init__()
        self.setWindowTitle("Smoke test - QtAgg")
        lay = QVBoxLayout(self)
        fig = Figure()
        self.canvas = FigureCanvas(fig)
        lay.addWidget(self.canvas)

        s = df.groupby("cycle_id")["utime_delta"].sum().sort_index()
        ax = fig.add_subplot(111)
        ax.plot(s.index, s.values, marker="o")
        ax.set_title("utime_delta sum by cycle")
        ax.set_xlabel("cycle_id"); ax.set_ylabel("value")
        self.canvas.draw()


if __name__ == "__main__":
    # 1) High-DPI 옵션(Windows 권장) + QApplication 단일 인스턴스
    app = QApplication.instance()
    if app is None:
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
        app = QApplication(sys.argv)

    # 2) 미니 DF
    n = 300
    df = pd.DataFrame({
        "pod_name": ["active"] * n,
        "comm": ["worker"] * n,
        "state": ["S"] * n,
        "utime_delta": np.random.randint(0, 10, size=n).astype(float),
    })
    df = ensure_cycle_id(df, cycle_size=100)

    # 3) 창 띄우기
    w = CyclePlotterApp(df)
    w.resize(640, 420)
    w.show()
    sys.exit(app.exec_())
