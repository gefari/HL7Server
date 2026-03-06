import asyncio
import sys
import hl7
from hl7.mllp import start_hl7_server

from PySide6.QtCore import QThread, Signal, QObject
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QPushButton,
    QTextEdit, QLabel, QStatusBar, QSplitter,
    QTableWidget, QTableWidgetItem, QHeaderView,
    QAbstractItemView
)

from PySide6.QtGui import QFont, QColor
from PySide6.QtCore import Qt

HOST = "0.0.0.0"
PORT = 8081

# ──────────────────────────────────────────────
# MDC code → human-readable label lookup
# Covers all codes seen in your MostCareUp ORU^R01 messages
# ──────────────────────────────────────────────
MDC_LABELS: dict[str, str] = {
    # IHE PCD / IEEE 11073 containment hierarchy
    "69941": "MDS (Multi-param Monitor)",
    "69942": "VMD (Virtual Med Device)",
    "69943": "Channel",
    # Patient demographics
    "68063": "Weight",
    "68060": "Height",
    # Hemodynamics – pressure
    "150037": "ABP Systolic",
    "150038": "ABP Diastolic",
    "150039": "ABP Mean",
    "150084": "CVP",
    # MostCareUp proprietary
    "1001":  "PDIC (dP/dt index corrected)",
    "1002":  "CCE (cardiac cycle efficiency)",
    "1003":  "SVV (stroke vol. variation)",
    "1006":  "PPV (pulse pressure var.)",
    "1007":  "SPV (systolic pressure var.)",
    "1008":  "DPV (diastolic pressure var.)",
    "1009":  "Ea (arterial elastance)",
    "1010":  "PPV/SVV ratio",
    "1011":  "SV/kg",
    "1012":  "Diastolic peak pressure",
    "1014":  "DO2i (O₂ delivery index)",
    "1015":  "PP (pulse pressure)",
    "1016":  "MAP–DIC",
    "1017":  "Ztot (aortic impedance)",
    "1019":  "CPI (cardiac power index)",
    # Cardiac output / flow
    "149514": "Heart Rate / Pulse Rate",
    "150276": "Cardiac Output",
    "150312": "SVR (syst. vasc. resistance)",
    "150404": "Stroke Volume",
    "149760": "SVRI",
    "149772": "Cardiac Index",
    "150636": "Stroke Volume Index",
    "150412": "Cardiac Work",
    # Ventilation / oxygen
    "150504": "dP/dt max (LV)",
    "153148": "O₂ Delivery (DO₂)",
}

# MDC unit code → symbol
MDC_UNITS: dict[str, str] = {
    "263875": "kg",
    "263441": "cm",
    "266016": "mmHg",
    "264864": "bpm",
    "270656": "dyn·s/cm⁵",
    "267616": "IU",
    "264216": "L/min",
    "263762": "mL",
    "262688": "%",
    "273120": "mmHg/s",
    "264992": "L/min/m²",
    "270464": "dyn·s·m²/cm⁵",
    "263570": "mL/m²",
    "265216": "L/min",
    "265010": "mL/min/m²",
    "268480": "mmHg/L",
    "265330": "mL/kg",
    "270688": "mmHg·s/mL",
    "266176": "W",
}


def decode_obx_segments(msg: hl7.Message) -> list[dict]:
    """Return a list of dicts with decoded OBX fields."""
    rows = []
    try:
        obx_list = msg.segments("OBX")
    except KeyError:
        return rows

    for obx in obx_list:
        try:
            value_type = str(obx[2]).strip()
            # OBX-3: observation identifier  e.g. "150037^MDC_PRESS_BLD_ART_ABP_SYS^MDC"
            obs_id_field = str(obx[3])
            obs_id_parts = obs_id_field.split("^")
            code        = obs_id_parts[0].strip()
            mnemonic    = obs_id_parts[1].strip() if len(obs_id_parts) > 1 else ""
            label       = MDC_LABELS.get(code, mnemonic or code)

            # OBX-4: sub-ID (containment path)
            sub_id = str(obx[4]).strip()

            # OBX-5: value (only meaningful for NM / ST types)
            value = str(obx[5]).strip() if value_type == "NM" else "—"

            # OBX-6: units  e.g. "266016^MDC_DIM_MMHG^MDC"
            unit_field = str(obx[6])
            unit_parts = unit_field.split("^")
            unit_code  = unit_parts[0].strip()
            unit_label = unit_parts[1].strip() if len(unit_parts) > 1 else unit_code
            unit       = MDC_UNITS.get(unit_code, unit_label) if unit_code else ""

            # OBX-11: result status
            status = str(obx[11]).strip()

            rows.append({
                "code":    code,
                "label":   label,
                "sub_id":  sub_id,
                "value":   value,
                "unit":    unit,
                "status":  status,
                "type":    value_type,
            })
        except (IndexError, KeyError):
            continue
    return rows


# ──────────────────────────────────────────────
# Signals bridge: asyncio → Qt
# ──────────────────────────────────────────────
class ServerSignals(QObject):
    log_message = Signal(str)
    status_changed = Signal(str)   # "running" | "stopped" | "error"
    obx_decoded = Signal(list)  # list[dict] – one per OBX row


# ──────────────────────────────────────────────
# Asyncio HL7 Server (runs in a QThread)
# ──────────────────────────────────────────────
class HL7ServerThread(QThread):
    def __init__(self, signals: ServerSignals):
        super().__init__()
        self.signals = signals
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None          # ← NEW

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve())
        except Exception as e:
            self.signals.log_message.emit(f"[ERROR] {e}")
            self.signals.status_changed.emit("error")
        finally:
            # ── Graceful shutdown: cancel all pending tasks ── ← NEW
            try:
                pending = asyncio.all_tasks(self._loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            finally:
                self._loop.close()

    async def _serve(self):
        signals = self.signals
        self._stop_event = asyncio.Event()                     # ← NEW

        async def handle_connection(reader, writer):
            peer = writer.get_extra_info("peername")
            signals.log_message.emit(f"[CONNECT] {peer}")
            try:
                while not writer.is_closing():
                    msg: hl7.Message = await reader.readmessage()
                    msg_ctrl_id = str(msg.segment("MSH")[10])
                    msg_type    = str(msg.segment("MSH")[9])

                    signals.log_message.emit(
                        f"[RX] {peer} | Type: {msg_type} | ID: {msg_ctrl_id}"
                    )

                    raw_payload = str(msg)
                    readable    = raw_payload.replace("\r", "\n").strip()
                    sep         = "─" * 60
                    signals.log_message.emit(
                        f"[PAYLOAD]\n{sep}\n{readable}\n{sep}"
                    )

                    rows = decode_obx_segments(msg)
                    if rows:
                        signals.obx_decoded.emit(rows)

                    ack = msg.create_ack()
                    writer.writemessage(ack)
                    await writer.drain()
                    signals.log_message.emit(
                        f"[ACK] Sent AA to {peer} for msg {msg_ctrl_id}"
                    )

            except asyncio.IncompleteReadError:
                pass
            except asyncio.CancelledError:                     # ← NEW
                pass
            finally:
                try:                                           # ← NEW
                    if not writer.is_closing():
                        writer.close()
                        await writer.wait_closed()
                except Exception:                              # ← NEW
                    pass                                       # ← NEW
                signals.log_message.emit(f"[DISCONNECT] {peer}")

        async with await start_hl7_server(
            handle_connection, host=HOST, port=PORT
        ) as server:
            signals.log_message.emit(f"[SERVER] Listening on {HOST}:{PORT}")
            signals.status_changed.emit("running")
            await self._stop_event.wait()                      # ← NEW (replaces serve_forever)
            server.close()                                     # ← NEW

    def stop(self):
        if self._loop and self._stop_event and self._loop.is_running():
            # Signal the coroutine to exit cleanly via the event  ← NEW
            self._loop.call_soon_threadsafe(self._stop_event.set)
        self.signals.status_changed.emit("stopped")


# ──────────────────────────────────────────────
# Decoded OBX table widget
# ──────────────────────────────────────────────
COLUMNS = ["#", "Code", "Parameter", "Sub-ID", "Value", "Unit", "Status"]

class ObxTableWidget(QTableWidget):
    def __init__(self):
        super().__init__(0, len(COLUMNS))
        self.setHorizontalHeaderLabels(COLUMNS)
        self.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setAlternatingRowColors(True)
        self.setFont(QFont("Courier New", 9))
        self.verticalHeader().setVisible(False)

    def populate(self, rows: list[dict]):
        self.setRowCount(0)
        for r in rows:
            row_idx = self.rowCount()
            self.insertRow(row_idx)
            cells = [
                r["type"] if r["type"] else "—",
                r["code"],
                r["label"],
                r["sub_id"],
                r["value"],
                r["unit"],
                r["status"],
            ]
            for col, text in enumerate(cells):
                item = QTableWidgetItem(text)
                item.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter if col != 2
                    else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                )
                # Colour containment rows (X type = hierarchy node) gray
                if r["type"] == "X" or r["value"] == "—":
                    item.setForeground(QColor("#888888"))
                self.setItem(row_idx, col, item)

# ──────────────────────────────────────────────
# Main Window  — fix the C++ object deleted crash
# ──────────────────────────────────────────────
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"HL7 MLLP Server — Port {PORT}")
        self.resize(800, 640)

        self.signals = ServerSignals()
        self.signals.log_message.connect(self._append_log)
        self.signals.status_changed.connect(self._on_status)
        self.signals.obx_decoded.connect(self._on_obx_decoded)

        self.server_thread: HL7ServerThread | None = None

        # ── Widgets ──
        self.log_view = QTextEdit(readOnly=True)
        self.log_view.setFont(QFont("Courier New", 9))

        self.obx_table = ObxTableWidget()

        # Give the splitter an explicit parent so PySide6 does NOT
        # take exclusive ownership and delete our widget references ← NEW
        splitter = QSplitter(Qt.Orientation.Vertical, self)   # ← NEW parent=self
        splitter.addWidget(self.log_view)
        splitter.addWidget(self.obx_table)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 3)

        self.btn_start = QPushButton("▶  Start Server")
        self.btn_stop  = QPushButton("■  Stop Server")
        self.btn_clear = QPushButton("🗑  Clear")
        self.btn_stop.setEnabled(False)

        self.status_label = QLabel("● Stopped")
        self.status_label.setStyleSheet("color: gray; font-weight: bold;")

        btn_row = QHBoxLayout()
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        btn_row.addWidget(self.btn_clear)
        btn_row.addStretch()
        btn_row.addWidget(self.status_label)

        layout = QVBoxLayout()
        layout.addLayout(btn_row)
        layout.addWidget(splitter)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        self.statusBar().showMessage(f"Ready — will bind to {HOST}:{PORT}")

        self.btn_start.clicked.connect(self._start_server)
        self.btn_stop.clicked.connect(self._stop_server)
        self.btn_clear.clicked.connect(self._clear_all)

    def _start_server(self):
        self.server_thread = HL7ServerThread(self.signals)
        self.server_thread.start()
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)

    def _stop_server(self):
        if self.server_thread:
            self.server_thread.stop()
            self.server_thread.wait(5000)                      # ← NEW timeout ms
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

    def _clear_all(self):
        self.log_view.clear()
        self.obx_table.setRowCount(0)

    def _append_log(self, text: str):
        self.log_view.append(text)

    def _on_obx_decoded(self, rows: list):
        # Guard against late-arriving signals after widget is gone ← NEW
        try:
            self.obx_table.populate(rows)
        except RuntimeError:
            pass

    def _on_status(self, state: str):
        colors = {"running": "green", "stopped": "gray", "error": "red"}
        icons  = {"running": "▶", "stopped": "■", "error": "✖"}
        self.status_label.setText(f"{icons.get(state,'■')}  {state.capitalize()}")
        self.status_label.setStyleSheet(
            f"color: {colors.get(state,'gray')}; font-weight: bold;"
        )
        self.statusBar().showMessage(f"Server {state}")

    def closeEvent(self, event):
        self.signals.obx_decoded.disconnect(self._on_obx_decoded)  # ← NEW
        self.signals.log_message.disconnect(self._append_log)       # ← NEW
        self._stop_server()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())