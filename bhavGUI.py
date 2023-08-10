import sys
from time import sleep

from PyQt6 import QtGui, QtCore, QtWidgets
from PyQt6.QtCore import Qt, QObject, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QMainWindow,
    QApplication,
    QPushButton,
    QCheckBox,
    QProgressBar,
    QMessageBox,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QLineEdit,
)

import DataManager


# https://realpython.com/python-pyqt-qthread/
class Worker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal(int)

    def run(self):
        "running task"
        for i in range(5):
            sleep(5)
            self.progress.emit(i + 1)
        self.finished.emit()


class Window(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Bhav Downloader")
        self.generalLayout = QVBoxLayout()
        centralWidget = QWidget(self)
        centralWidget.setLayout(self.generalLayout)
        self.setCentralWidget(centralWidget)
        self.createDisplay()
        self.connectSignalAndSlots()
        self.progress.setValue(0)
        self.show()

    def createDisplay(self):
        self.progress = QProgressBar()
        self.btn = QPushButton("Download")
        self.generalLayout.addWidget(self.progress)
        self.generalLayout.addWidget(self.btn)

    def connectSignalAndSlots(self):
        self.btn.clicked.connect(self.incProgressBarValue)

    def setProgressBarValue(self, value):
        self.progress.setValue(value)

    def incProgressBarValue(self):
        self.setProgressBarValue((self.progress.value() + 5) % self.progress.maximum())

    def close_app(self):
        pass


def run():
    app = QApplication(sys.argv)
    window = Window()
    sys.exit(app.exec())


run()
