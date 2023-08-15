import typing
from PyQt5.QtWidgets import (
    QComboBox,
    QMainWindow,
    QApplication,
    QWidget,
    QCompleter,
    QLineEdit,
    QPushButton,
    QHBoxLayout,
    QVBoxLayout,
    QGridLayout,
    QFormLayout,
)

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QValidator
from PyQt5 import QtGui

import sqlalchemy as db
from matplotlib.backends.backend_qt5agg import FigureCanvas
import matplotlib.pyplot as plt
import matplotlib

import DataManager as dMgr
import config


class TagsValidator(QtGui.QValidator):
    def __init__(self, tags, *args, **kwargs):
        QtGui.QValidator.__init__(self, *args, **kwargs)
        self._tags = [tag.lower() for tag in tags]

    def validate(self, inputText, pos):
        if inputText.lower() in self._tags:
            return QtGui.QValidator.Acceptable
        len_ = len(inputText)
        for tag in self._tags:
            if tag[:len_] == inputText.lower():
                return QtGui.QValidator.Intermediate
        return QtGui.QValidator.Invalid


class tickerLineEdit(QLineEdit):
    def __init__(self, dataMgr=None):
        super().__init__()
        self.dataMgr = dataMgr
        self.data = []
        self.setPlaceholderText("Enter ticker name...")
        self.completer = QCompleter(self.data, self)
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.setCompleter(self.completer)
        self.set_autocompletion()

    def set_autocompletion(self):
        if self.dataMgr == None:
            tickers = []
        else:
            l = self.dataMgr.get_all_tickers()
            tickers = l["symbol"].to_list()
        self.data = tickers
        self.set_completion_list(tickers)
        # self.setValidator(TagsValidator(tickers))

    def text_changed(self):
        print("text changed ", self.text())

    def get_completion_list(self):
        return self.data

    def set_completion_list(self, data):
        self.data = data
        self.completer.model().setStringList(self.data)


class periodComboBox(QComboBox):
    def __init__(self):
        super().__init__()
        self.addItems([str(i) + "y" for i in range(1, 20)])


class customePushButton(QPushButton):
    def __init__(self):
        super().__init__()
        self.setText("Add to list")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # datamgr
        self.session = db.orm.Session(config.SQL_CON)
        self.dataMgr = dMgr.DataManager(self.session)

        self.setWindowTitle("Bhav Data viewer")

        self.tickerBox = tickerLineEdit(self.dataMgr)

        self.plotBtn = customePushButton()
        self.plotBtn.setText("Plot")
        self.plotBtn.clicked.connect(self.update_chart)
        self.btn2 = customePushButton()
        self.btn2.setText("test2")

        # combobox
        self.periodCombo = periodComboBox()

        # canvas
        self.canvas = FigureCanvas(plt.Figure(figsize=(15, 6)))

        # layout

        self.generalLayout = QHBoxLayout()
        self.menuBar1 = QFormLayout()

        self.generalLayout.addLayout(self.menuBar1)
        self.generalLayout.addWidget(self.canvas)

        self.menuBar1.addRow("Ticker", self.tickerBox)
        self.menuBar1.addRow("Period", self.periodCombo)
        self.menuBar1.addRow(self.plotBtn)

        centralWidget = QWidget(self)
        centralWidget.setLayout(self.generalLayout)
        self.setCentralWidget(centralWidget)
        self.insert_ax()

    def insert_ax(self):
        # font = {"weight": "normal", "size": 16}
        # matplotlib.rc("font", **font)
        self.ax = self.canvas.figure.subplots()
        self.bar = None

    def update_chart(self):
        ticker = self.tickerBox.text()
        print(ticker)
        self.ax.clear()
        self.ax.set_title(ticker)
        self.dataMgr.plot_equity(ticker, ax=self.ax)
        self.canvas.draw()


if __name__ == "__main__":
    app = QApplication([])
    w = MainWindow()
    w.show()
    app.exec()
