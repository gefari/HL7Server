import asyncio
import sys
import hl7
from hl7.mllp import start_hl7_server

from PySide6.QtCore import QThread, Signal, QObject
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QPushButton,
    QTextEdit, QLabel, QStatusBar
)
from PySide6.QtGui import QFont

HOST = "0.0.0.0"
PORT = 8081


# ──────────────────────────────────────────────
# Signals bridge: asyncio → Qt
# ──────────────────────────────────────────────
class ServerSignals(QObject):
    log_message = Signal(str)
    status_changed = Signal(str)   # "running" | "stopped" | "error"


# ──────────────────────────────────────────────
# Asyncio HL7 Server (runs in a QThread)
# ──────────────────────────────────────────────
class HL7ServerThread(QThread):
    def __init__(self, signals: ServerSignals):
        super().__init__()
        self.signals = signals
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server_task = None

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve())
        except Exception as e:
            self.signals.log_message.emit(f"[ERROR] {e}")
            self.signals.status_changed.emit("error")
        finally:
            self._loop.close()

    async def _serve(self):
        signals = self.signals

        async def handle_connection(reader, writer):
            peer = writer.get_extra_info("peername")
            signals.log_message.emit(f"[CONNECT] {peer}")
            try:
                while not writer.is_closing():
                    msg: hl7.Message = await reader.readmessage()
                    msg_ctrl_id = str(msg.segment("MSH")[10])  # MSH-10
                    msg_type    = str(msg.segment("MSH")[9])   # MSH-9
                    signals.log_message.emit(
                        f"[RX] {peer} | Type: {msg_type} | ID: {msg_ctrl_id}"
                    )
                    # Build and send ACK
                    ack = msg.create_ack()
                    writer.writemessage(ack)
                    await writer.drain()
                    signals.log_message.emit(
                        f"[ACK] Sent AA to {peer} for msg {msg_ctrl_id}"
                    )
            except asyncio.IncompleteReadError:
                pass
            finally:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
                signals.log_message.emit(f"[DISCONNECT] {peer}")

        async with await start_hl7_server(
            handle_connection, host=HOST, port=PORT
        ) as server:
            self.signals.log_message.emit(
                f"[SERVER] Listening on {HOST}:{PORT}"
            )
            self.signals.status_changed.emit("running")
            await server.serve_forever()

    def stop(self):
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self.signals.status_changed.emit("stopped")


# ──────────────────────────────────────────────
# PySide6 Main Window
# ──────────────────────────────────────────────
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"HL7 MLLP Server — Port {PORT}")
        self.resize(700, 480)

        self.signals = ServerSignals()
        self.signals.log_message.connect(self._append_log)
        self.signals.status_changed.connect(self._on_status)

        self.server_thread: HL7ServerThread | None = None

        # ── Widgets ──
        self.log_view = QTextEdit(readOnly=True)
        self.log_view.setFont(QFont("Courier New", 10))

        self.btn_start = QPushButton("▶  Start Server")
        self.btn_stop  = QPushButton("■  Stop Server")
        self.btn_stop.setEnabled(False)

        self.status_label = QLabel("● Stopped")
        self.status_label.setStyleSheet("color: gray; font-weight: bold;")

        btn_row = QHBoxLayout()
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        btn_row.addStretch()
        btn_row.addWidget(self.status_label)

        layout = QVBoxLayout()
        layout.addLayout(btn_row)
        layout.addWidget(self.log_view)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.statusBar().showMessage(f"Ready — will bind to {HOST}:{PORT}")

        self.btn_start.clicked.connect(self._start_server)
        self.btn_stop.clicked.connect(self._stop_server)

    def _start_server(self):
        self.server_thread = HL7ServerThread(self.signals)
        self.server_thread.start()
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)

    def _stop_server(self):
        if self.server_thread:
            self.server_thread.stop()
            self.server_thread.wait()
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

    def _append_log(self, text: str):
        self.log_view.append(text)

    def _on_status(self, state: str):
        colors = {"running": "green", "stopped": "gray", "error": "red"}
        icons  = {"running": "▶", "stopped": "■", "error": "✖"}
        color  = colors.get(state, "gray")
        icon   = icons.get(state, "■")
        self.status_label.setText(f"{icon}  {state.capitalize()}")
        self.status_label.setStyleSheet(
            f"color: {color}; font-weight: bold;"
        )
        self.statusBar().showMessage(f"Server {state}")

    def closeEvent(self, event):
        self._stop_server()
        event.accept()


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())