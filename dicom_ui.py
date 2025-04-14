import asyncio
import threading
from tkinter import *
from tkinter.scrolledtext import ScrolledText
import logging
from main import DICOMReceiver  # make sure DICOMReceiver is imported properly

class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.insert(END, msg + '\n')
        self.text_widget.see(END)

class DICOMApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DICOM Retriever UI")
        self.receiver = None
        self.server_running = False

        # Input Fields
        Label(root, text="Server IP:").pack()
        self.ip_entry = Entry(root, width=40)
        self.ip_entry.insert(0, "0.0.0.0")
        self.ip_entry.pack()

        Label(root, text="Port:").pack()
        self.port_entry = Entry(root, width=40)
        self.port_entry.insert(0, "11112")
        self.port_entry.pack()

        Label(root, text="AE Title:").pack()
        self.ae_entry = Entry(root, width=40)
        self.ae_entry.insert(0, "STREAMSCP")
        self.ae_entry.pack()

        # Control Buttons
        self.start_button = Button(root, text="Start DICOM Server", command=self.start_server)
        self.start_button.pack(pady=5)

        self.stop_button = Button(root, text="Stop DICOM Server", command=self.stop_server, state=DISABLED)
        self.stop_button.pack(pady=5)

        # Logs
        self.log = ScrolledText(root, width=80, height=20)
        self.log.pack(padx=10, pady=10)

        # Redirect logs to GUI
        text_handler = TextHandler(self.log)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        text_handler.setFormatter(formatter)
        logging.getLogger().addHandler(text_handler)
        logging.getLogger().setLevel(logging.INFO)

        self.log_message("Enter server config and click 'Start DICOM Server'.")

    def start_server(self):
        if self.server_running:
            self.log_message("[INFO] Server is already running.")
            return

        ip = self.ip_entry.get().strip()
        port = int(self.port_entry.get().strip())
        ae_title = self.ae_entry.get().strip()

        self.log_message(f"Starting DICOM server on {ip}:{port} with AE Title '{ae_title}'...")

        try:
            self.receiver = DICOMReceiver(server_ip=ip, server_port=port, local_ae_title=ae_title)
            thread = threading.Thread(target=self.run_server, daemon=True)
            thread.start()
            self.server_running = True
            self.start_button.config(state=DISABLED)
            self.stop_button.config(state=NORMAL)
        except Exception as e:
            self.log_message(f"[ERROR] Failed to start server: {str(e)}")

    def stop_server(self):
        if self.receiver and self.receiver.running:
            self.receiver.running = False
            self.log_message("[INFO] Stopping DICOM server...")
            self.start_button.config(state=NORMAL)
            self.stop_button.config(state=DISABLED)
        else:
            self.log_message("[INFO] Server is not running.")

    def run_server(self):
        asyncio.run(self.receiver.start())

    def log_message(self, msg):
        self.log.insert(END, msg + "\n")
        self.log.see(END)

if __name__ == "__main__":
    root = Tk()
    app = DICOMApp(root)
    root.mainloop()
