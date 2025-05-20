from gui import GridFlowGUI
from PyQt5.QtWidgets import QApplication
import sys

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = GridFlowGUI()
    window.show()
    sys.exit(app.exec_())

