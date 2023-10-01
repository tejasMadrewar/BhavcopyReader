from PyQt5.QtWidgets import (
    QComboBox,
    QMainWindow,
    QApplication,
    QWidget,
    QTableView,
    QCompleter,
    QLineEdit,
    QPushButton,
    QHBoxLayout,
    QVBoxLayout,
    QTabWidget,
    QGridLayout,
    QGroupBox,
    QFormLayout,
    QDateEdit,
)

from PyQt5 import QtCore
from PyQt5.QtCore import Qt, QAbstractTableModel, QModelIndex

import sqlalchemy as db
from matplotlib.backends.backend_qt5agg import FigureCanvas
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np

from DataManager import DataManager
import config


from datetime import datetime, timedelta


class PandasModel(QAbstractTableModel):
    """A model to interface a Qt view with pandas dataframe"""

    def __init__(self, dataframe: pd.DataFrame, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._dataframe = dataframe

    def rowCount(self, parent=QModelIndex()) -> int:
        """Override method from QAbstractTableModel

        Return row count of the pandas DataFrame
        """
        if parent == QModelIndex():
            return len(self._dataframe)

        return 0

    def columnCount(self, parent=QModelIndex()) -> int:
        """Override method from QAbstractTableModel

        Return column count of the pandas DataFrame
        """
        if parent == QModelIndex():
            return len(self._dataframe.columns)
        return 0

    def data(self, index: QModelIndex, role=Qt.ItemDataRole):
        """Override method from QAbstractTableModel

        Return data cell from the pandas DataFrame
        """
        if not index.isValid():
            return None

        if role == Qt.DisplayRole:
            return str(self._dataframe.iloc[index.row(), index.column()])

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole
    ):
        """Override method from QAbstractTableModel

        Return dataframe index as vertical header data and columns as horizontal header data.
        """
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._dataframe.columns[section])

            if orientation == Qt.Vertical:
                return str(self._dataframe.index[section])

        return None


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

    def text_changed(self):
        print("text changed ", self.text())

    def get_completion_list(self):
        return self.data

    def set_completion_list(self, data):
        self.data = data
        self.completer.model().setStringList(self.data)


class datePeriodBox(QWidget):
    def __init__(self):
        super().__init__()
        self.cBox = QComboBox()
        self.cBox.addItems([str(i) + "y" for i in range(1, 20)])
        self.boxLayout = QVBoxLayout()
        self.boxLayout.addWidget(self.cBox)
        self.setLayout(self.boxLayout)

    def getTimeRange(self):
        year = self.get_years()
        today = datetime.now().date()
        delta = timedelta(days=year * 365)
        return (today - delta, today)

    def get_years(self) -> int:
        t = self.cBox.currentText()
        return int(t[:-1])

    def reset(self):
        self.cBox.setEditText("1y")


class dateRangeBox(QWidget):
    def __init__(self):
        super().__init__()
        self.defaultTo = datetime.now()
        self.defaultFrom = datetime.now() - timedelta(days=1 * 365)
        self.boxLayout = QFormLayout()

        self.fromDatePicker = QDateEdit(calendarPopup=True)
        self.fromDatePicker.setDateTime(self.defaultFrom)
        self.fromDatePicker.setMaximumDate(self.defaultTo)

        self.toDatePicker = QDateEdit(calendarPopup=True)
        self.toDatePicker.setDateTime(self.defaultTo)
        self.toDatePicker.setMaximumDate(self.defaultTo)

        self.boxLayout.addRow("From:", self.fromDatePicker)
        self.boxLayout.addRow("To:", self.toDatePicker)

        self.setLayout(self.boxLayout)

    def getTimeRange(self):
        return (
            self.fromDatePicker.date().toPyDate(),
            self.toDatePicker.date().toPyDate(),
        )

    def reset(self):
        self.fromDatePicker.setDateTime(self.defaultFrom)
        self.toDatePicker.setDateTime(self.defaultTo)


class timeFilterBox(QTabWidget):
    def __init__(self):
        super().__init__()
        self.datePeriodBox = datePeriodBox()
        self.dateRangeBox = dateRangeBox()
        self.addTab(self.datePeriodBox, "Period")
        self.addTab(self.dateRangeBox, "Range")
        self.setDocumentMode(True)
        # self.currentChanged.connect(self.test)

    def getTimeRange(self):
        return self.currentWidget().getTimeRange()

    def reset(self):
        pass


class customePushButton(QPushButton):
    def __init__(self):
        super().__init__()
        self.setText("Add to list")


class CorpActionWidget(QTableView):
    def __init__(self, dataMgr: DataManager):
        super().__init__()
        self.dataMgr = dataMgr
        self.setWindowTitle("Corp Actions")
        self.resize(700, 500)
        self.horizontalHeader().setStretchLastSection(True)
        self.setAlternatingRowColors(True)
        self.model = PandasModel(pd.DataFrame())
        self.setModel(self.model)

    def showTable(self):
        self.show()

    def setTicker(self, ticker: str):
        df = self.dataMgr.get_corpAction_data(ticker)
        l = [
            "short_name",
            "Ex_date",
            "action",
            "A",
            "B",
            "C",
            "Purpose",
        ]
        df = df[l].replace({np.nan: "-"})
        df.sort_values(by=["short_name", "Ex_date", "action"], inplace=True)
        self.model = PandasModel(df)
        self.setModel(self.model)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # datamgr
        self.session = db.orm.Session(config.SQL_CON)
        self.dataMgr = DataManager(self.session)

        self.setWindowTitle("Bhav Data viewer")

        self.tickerBox = tickerLineEdit(self.dataMgr)
        self.corpActionTable = CorpActionWidget(self.dataMgr)
        self.periodFilter = timeFilterBox()

        self.plotBtn = customePushButton()
        self.plotBtn.setText("Plot")
        self.plotBtn.clicked.connect(self.update_chart)
        self.showCorpActionBtn = customePushButton()
        self.showCorpActionBtn.setText("show corpAction")
        self.showCorpActionBtn.clicked.connect(self.show_corp_action)

        # canvas
        self.canvas = FigureCanvas(plt.Figure(figsize=(15, 6)))

        # layout

        self.generalLayout = QHBoxLayout()
        self.menuBar1 = QFormLayout()

        self.generalLayout.addLayout(self.menuBar1)
        self.generalLayout.addWidget(self.canvas)

        self.menuBar1.addRow("Ticker", self.tickerBox)
        self.menuBar1.addRow(self.periodFilter)
        self.menuBar1.addRow(self.plotBtn)
        self.menuBar1.addRow(self.showCorpActionBtn)

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
        dates = self.periodFilter.getTimeRange()
        print(ticker, dates)
        self.ax.clear()
        self.ax.set_title(ticker)
        self.dataMgr.plot_equity(
            ticker, fromDate=min(dates), toDate=max(dates), ax=self.ax
        )
        self.canvas.draw()

    def show_corp_action(self):
        self.corpActionTable.setTicker(self.tickerBox.text())
        self.corpActionTable.show()


if __name__ == "__main__":
    app = QApplication([])
    w = MainWindow()
    w.show()
    app.exec()
