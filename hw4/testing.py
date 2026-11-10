import os
import queue
import re
import socket
import struct
import subprocess
import sys
import threading
import time
import tkinter as tk
import uuid
from pathlib import Path
from tkinter import messagebox, ttk

HOST = "127.0.0.1"
PORTS = (5000, 5001, 5002, 5003, 5004)
ECHO_FRAME = struct.pack(">BBHH", 4, 0, len(b"hello"), 0) + b"hello"
ECHO_RESPONSE = b"ifmmp"
CLIENT_IDENTIFY = 2
CLIENT_CONNECTION = 1
PROBE_TIMEOUT = 3
MAX_ACTIVE_PROBES = 5
SERVER_SCRIPT = Path(__file__).with_name("Sr4.py")
CLIENT_SCRIPT = Path(__file__).with_name("Cl4.py")


class NetworkWindow:
    """Launch the lab servers and clients and draw their live connections."""

    def __init__(self, root):
        self.root = root
        self.root.title("TCP Network Visualizer")
        self.root.geometry("1100x720")
        self.events = queue.Queue()
        self.server_processes = [None] * len(PORTS)
        self.client_processes = [None] * len(PORTS)
        self.server_edges = set()
        self.network_connected_ports = set()
        self.client_edges = {}
        self.client_test_edges = {}
        self.client_probe_rtts = [{} for _ in PORTS]
        self.client_probe_cancel = [None] * len(PORTS)
        self.client_probe_tokens = [0] * len(PORTS)
        self.client_probe_sockets = [set() for _ in PORTS]
        self.client_probe_pending = [[] for _ in PORTS]
        self.client_probe_checked = [set() for _ in PORTS]
        self.client_probe_best = [None] * len(PORTS)
        self.client_probe_workers = [False] * len(PORTS)
        self.probe_lock = threading.Lock()
        self.probe_semaphore = threading.BoundedSemaphore(MAX_ACTIVE_PROBES)
        self.server_status = ["Stopped"] * len(PORTS)
        self.client_status = ["Stopped"] * len(PORTS)
        self.server_search = []
        self.server_buttons = []
        self.client_buttons = []
        self.server_port_vars = []
        self.client_name_vars = []
        self.client_port_vars = []
        self.client_message_vars = []

        self._build_layout()
        self.root.after(100, self._process_events)
        self.root.protocol("WM_DELETE_WINDOW", self._close)

    def _build_layout(self):
        """Create the server/client controls and graph canvas."""
        body = ttk.Frame(self.root, padding=10)
        body.pack(fill="both", expand=True)
        controls = ttk.Frame(body, width=370)
        controls.pack(side="left", fill="y", padx=(0, 10))
        graph = ttk.Frame(body)
        graph.pack(side="right", fill="both", expand=True)

        ttk.Label(controls, text="Servers", font=("", 12, "bold")).pack(anchor="w")
        server_rows = ttk.Frame(controls)
        server_rows.pack(fill="x", pady=(5, 12))
        for index, port in enumerate(PORTS):
            row = ttk.Frame(server_rows)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=f"Server {index}", width=10).pack(side="left")
            port_var = tk.StringVar(value=str(port))
            self.server_port_vars.append(port_var)
            ttk.Entry(row, textvariable=port_var, width=7).pack(side="left", padx=3)
            search_var = tk.BooleanVar(value=True)
            self.server_search.append(search_var)
            ttk.Checkbutton(row, text="Search", variable=search_var).pack(side="left")
            button = ttk.Button(row, text="Start", command=lambda i=index: self.start_server(i))
            button.pack(side="right")
            self.server_buttons.append(button)
        ttk.Button(
            server_rows,
            text="Start all servers",
            command=lambda: self._start_all("server", 0),
        ).pack(anchor="e", pady=(4, 0))

        ttk.Label(controls, text="Clients", font=("", 12, "bold")).pack(anchor="w")
        client_rows = ttk.Frame(controls)
        client_rows.pack(fill="x", pady=(5, 10))
        for index in range(len(PORTS)):
            row = ttk.Frame(client_rows)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=f"Client {index}", width=10).pack(side="left")
            name_var = tk.StringVar(value=f"client{index}")
            self.client_name_vars.append(name_var)
            ttk.Entry(row, textvariable=name_var, width=11).pack(side="left", padx=2)
            port_var = tk.StringVar(value=str(PORTS[index]))
            self.client_port_vars.append(port_var)
            ttk.Entry(row, textvariable=port_var, width=7).pack(side="left", padx=2)
            button = ttk.Button(row, text="Start", command=lambda i=index: self.start_client(i))
            button.pack(side="right")
            self.client_buttons.append(button)

            message_row = ttk.Frame(client_rows)
            message_row.pack(fill="x", padx=(78, 0), pady=(0, 4))
            message_var = tk.StringVar()
            self.client_message_vars.append(message_var)
            message_entry = ttk.Entry(message_row, textvariable=message_var)
            message_entry.pack(side="left", fill="x", expand=True)
            message_entry.bind("<Return>", lambda event, i=index: self.send_client_message(i))
            ttk.Button(
                message_row,
                text="Send",
                command=lambda i=index: self.send_client_message(i),
            ).pack(side="right", padx=(3, 0))
        ttk.Button(
            client_rows,
            text="Start all clients",
            command=lambda: self._start_all("client", 0),
        ).pack(anchor="e", pady=(4, 0))

        ttk.Label(
            controls,
            text="Client fields: username and server port.\n"
            "Message format: recipient:message\n"
            "RTT checks visit each server once; at most five checks run at once.",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        ttk.Label(controls, text="Activity", font=("", 12, "bold")).pack(anchor="w")
        self.log = tk.Text(controls, width=43, height=16, state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True, pady=(5, 0))

        ttk.Label(
            graph,
            text="Connections (green: selected server; purple: RTT test)",
            font=("", 12, "bold"),
        ).pack(anchor="w")
        self.canvas = tk.Canvas(graph, background="white", highlightthickness=1)
        self.canvas.pack(fill="both", expand=True, pady=(5, 0))
        self.canvas.bind("<Configure>", lambda event: self.draw_graph())
        self.draw_graph()

    def _start_process(self, kind, index, script, inputs):
        """Start one script and send its setup answers to its input stream."""
        processes = self.server_processes if kind == "server" else self.client_processes
        buttons = self.server_buttons if kind == "server" else self.client_buttons
        if processes[index] is not None:
            return

        process = None
        try:
            process = subprocess.Popen(
                [sys.executable, "-u", str(script)],
                cwd=str(script.parent),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            process.stdin.write(inputs)
            process.stdin.flush()
        except (OSError, ValueError) as error:
            if process is not None:
                process.terminate()
            messagebox.showerror("Could not start", str(error))
            return

        processes[index] = process
        buttons[index].configure(text="Stop", command=lambda i=index: self.stop_process(kind, i))
        statuses = self.server_status if kind == "server" else self.client_status
        statuses[index] = "Starting"
        threading.Thread(
            target=self._read_output,
            args=(kind, index, process),
            daemon=True,
        ).start()
        self.draw_graph()

    def _start_all(self, kind, index):
        """Start all configured servers or clients in slot order."""
        if index >= len(PORTS):
            return
        if kind == "server":
            self.start_server(index)
        else:
            self.start_client(index)
        self.root.after(350, lambda: self._start_all(kind, index + 1))

    def start_server(self, index):
        """Validate and launch the selected server slot."""
        try:
            port = int(self.server_port_vars[index].get())
            if port not in PORTS or port in [
                int(value.get()) for i, value in enumerate(self.server_port_vars)
                if i != index and value.get().isdigit()
            ]:
                raise ValueError
        except ValueError:
            messagebox.showerror("Invalid server port", f"Use a unique port from {PORTS}.")
            return
        search = "y" if self.server_search[index].get() else "n"
        self._start_process(
            "server",
            index,
            SERVER_SCRIPT,
            f"{PORTS.index(port)}\n{search}\n",
        )

    def start_client(self, index):
        """Validate and launch the selected client slot."""
        name = self.client_name_vars[index].get().strip()
        try:
            port = int(self.client_port_vars[index].get())
            if not name or not 1 <= port <= 65535:
                raise ValueError
        except ValueError:
            messagebox.showerror("Invalid client", "Enter a username and valid server port.")
            return
        self._start_process(
            "client",
            index,
            CLIENT_SCRIPT,
            f"{HOST}\n{port}\n{name}\n",
        )
        if self.client_processes[index] is None:
            return
        self.client_edges[index] = port
        self.client_probe_rtts[index].clear()
        self.draw_graph()
        self._start_client_rtt_checks(index, port)

    def _start_client_rtt_checks(self, index, initial_port):
        """Start the visualizer's sequential echo checks for one client."""
        self._cancel_client_rtt_checks(index)
        with self.probe_lock:
            self.client_probe_tokens[index] += 1
            token = self.client_probe_tokens[index]
            cancel = threading.Event()
            self.client_probe_cancel[index] = cancel
            self.client_probe_pending[index] = []
            self.client_probe_checked[index] = set()
            self.client_probe_best[index] = None
            self.client_probe_workers[index] = False
            for port in [initial_port] + [port for port in PORTS if port != initial_port]:
                self._enqueue_client_probe_locked(index, port)
        self._start_client_probe_worker(index, token, cancel)

    def _enqueue_client_probe_locked(self, index, port):
        """Queue a server once; caller must hold the probe lock."""
        if port in self.client_probe_checked[index]:
            return False
        self.client_probe_checked[index].add(port)
        self.client_probe_pending[index].append(port)
        return True

    def _queue_server_for_clients(self, port):
        """Ask every active client to test a newly connected server."""
        if port not in PORTS:
            return
        workers = []
        with self.probe_lock:
            if port in self.network_connected_ports:
                return
            self.network_connected_ports.add(port)
            for index, cancel in enumerate(self.client_probe_cancel):
                if cancel is None or cancel.is_set():
                    continue
                self.client_probe_checked[index].discard(port)
                if self._enqueue_client_probe_locked(index, port):
                    workers.append((index, self.client_probe_tokens[index], cancel))
        for index, token, cancel in workers:
            self._start_client_probe_worker(index, token, cancel)

    def _start_client_probe_worker(self, index, token, cancel):
        """Ensure one worker drains this client's queued RTT candidates."""
        with self.probe_lock:
            if self.client_probe_workers[index] or cancel.is_set():
                return
            if not self.client_probe_pending[index]:
                return
            self.client_probe_workers[index] = True
        threading.Thread(
            target=self._check_client_server_rtts,
            args=(index, token, cancel),
            daemon=True,
        ).start()

    def _check_client_server_rtts(self, index, token, cancel):
        """Probe queued servers and keep the fastest valid echo connection."""
        while not cancel.is_set():
            with self.probe_lock:
                if not self.client_probe_pending[index]:
                    self.client_probe_workers[index] = False
                    best = self.client_probe_best[index]
                    self.events.put((
                        "rtt",
                        index,
                        token,
                        "finished",
                        best[0] if best else None,
                    ))
                    return
                port = self.client_probe_pending[index].pop(0)
            if not self._acquire_probe_slot(cancel):
                break
            conn = None
            try:
                self.events.put(("rtt", index, token, "candidate", port))
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.settimeout(PROBE_TIMEOUT)
                if not self._track_probe_socket(index, conn, cancel):
                    conn = None
                    break
                conn.connect((HOST, port))
                username = f"__rtt_{index}_{token}_{uuid.uuid4().hex[:8]}".encode("ascii")
                identify_frame = (
                    struct.pack(">BBHH", CLIENT_IDENTIFY, CLIENT_CONNECTION, len(username), 0)
                    + username
                )
                conn.sendall(identify_frame)
                started = time.time()
                conn.sendall(ECHO_FRAME)
                header = self._recv_exact(conn, 6)
                if header is None:
                    raise ConnectionError("Server disconnected before its echo reply")
                msg_type, subtype, length, sublen = struct.unpack(">BBHH", header)
                payload = self._recv_exact(conn, length) if length else b""
                if (
                    payload is None
                    or msg_type != 4
                    or subtype != 1
                    or sublen != 0
                    or payload != ECHO_RESPONSE
                ):
                    raise ValueError("Invalid echo response")
                done = time.time()
                elapsed = done - started
                print(
                    f"[RTT] Client {index} -> {HOST}:{port}: "
                    f"{elapsed} seconds",
                    flush=True,
                )
                self.events.put((
                    "rtt",
                    index,
                    token,
                    "measured",
                    port,
                    elapsed,
                ))
                self.events.put((
                    "rtt",
                    index,
                    token,
                    "log",
                    f"[RTT] Client {index} -> {HOST}:{port}: {elapsed} seconds",
                ))
                if cancel.is_set():
                    break

                current_best = self.client_probe_best[index]
                if current_best is None or elapsed < current_best[1]:
                    previous_port = current_best[0] if current_best else None
                    previous_socket = current_best[2] if current_best else None
                    self.client_probe_best[index] = (port, elapsed, conn)
                    self.events.put(("rtt", index, token, "selected", port))
                    if previous_socket is not None:
                        self._close_probe_socket(index, previous_socket)
                        self.events.put((
                            "rtt",
                            index,
                            token,
                            "log",
                            f"[RTT] Client {index}: {port} is faster than "
                            f"{previous_port}; disconnecting from {previous_port} "
                            f"and switching to {port}.",
                        ))
                else:
                    self._close_probe_socket(index, conn)
                    self.events.put((
                        "rtt",
                        index,
                        token,
                        "log",
                        f"[RTT] Client {index}: {port} is slower; staying on "
                        f"{current_best[0]}.",
                    ))
            except (OSError, ValueError, struct.error, ConnectionError) as error:
                    if conn is not None:
                        self._close_probe_socket(index, conn)
                    if not cancel.is_set():
                        self.events.put((
                        "rtt",
                        index,
                        token,
                        "log",
                        f"[RTT] Client {index} -> {HOST}:{port} unavailable: {error}",
                    ))
            finally:
                self.probe_semaphore.release()

        if cancel.is_set():
            return

    def _acquire_probe_slot(self, cancel):
        """Wait for one of five global RTT slots, while remaining cancellable."""
        while not cancel.is_set():
            if self.probe_semaphore.acquire(timeout=0.1):
                if cancel.is_set():
                    self.probe_semaphore.release()
                    return False
                return True
        return False

    def _track_probe_socket(self, index, conn, cancel):
        """Track a probe socket so Stop can close it immediately."""
        with self.probe_lock:
            if cancel.is_set():
                conn.close()
                return False
            self.client_probe_sockets[index].add(conn)
            return True

    def _close_probe_socket(self, index, conn):
        """Remove and close one socket used by the RTT visualizer."""
        with self.probe_lock:
            self.client_probe_sockets[index].discard(conn)
        try:
            conn.close()
        except OSError:
            pass

    def _cancel_client_rtt_checks(self, index):
        """Cancel a client's RTT worker and close its tracked sockets."""
        cancel = self.client_probe_cancel[index]
        if cancel is not None:
            cancel.set()
        with self.probe_lock:
            sockets = list(self.client_probe_sockets[index])
            self.client_probe_sockets[index].clear()
            self.client_probe_pending[index].clear()
            self.client_probe_checked[index].clear()
            best = self.client_probe_best[index]
            self.client_probe_best[index] = None
            self.client_probe_workers[index] = False
            if best is not None:
                sockets.append(best[2])
        for conn in sockets:
            try:
                conn.close()
            except OSError:
                pass

    @staticmethod
    def _recv_exact(conn, size):
        """Read one complete protocol field, or return None on disconnect."""
        data = bytearray()
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    def send_client_message(self, index):
        """Send the message field to a running client's chat loop."""
        process = self.client_processes[index]
        text = self.client_message_vars[index].get()
        if process is None or process.poll() is not None:
            messagebox.showinfo("Client stopped", "Start this client before sending messages.")
            return
        try:
            process.stdin.write(text + "\n")
            process.stdin.flush()
            self.client_message_vars[index].set("")
        except (OSError, BrokenPipeError) as error:
            self._append_log(f"[Client {index}] Send failed: {error}")

    def stop_process(self, kind, index):
        """Stop a selected process and update its connection status."""
        processes = self.server_processes if kind == "server" else self.client_processes
        process = processes[index]
        if process is None:
            return
        if kind == "client" and process.poll() is None:
            try:
                process.stdin.write("exit\n")
                process.stdin.flush()
            except OSError:
                pass
        threading.Thread(target=self._terminate, args=(process,), daemon=True).start()
        processes[index] = None
        if kind == "server":
            self.server_status[index] = "Stopped"
            port = int(self.server_port_vars[index].get())
            self.network_connected_ports.discard(port)
            self.server_edges = {
                edge for edge in self.server_edges if port not in edge
            }
        else:
            self._cancel_client_rtt_checks(index)
            self.client_probe_tokens[index] += 1
            self.client_test_edges.pop(index, None)
            self.client_probe_rtts[index].clear()
            self.client_status[index] = "Stopped"
            self.client_edges.pop(index, None)
        self._set_button(kind, index, "Start")
        self.draw_graph()

    @staticmethod
    def _terminate(process):
        """Wait briefly for a requested exit, then force-stop the process."""
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()

    def _read_output(self, kind, index, process):
        """Read child output in the background and report its exit."""
        try:
            for line in process.stdout:
                self.events.put(("line", kind, index, line.rstrip()))
        except (OSError, ValueError) as error:
            self.events.put(("line", kind, index, f"[ERROR] Output reader: {error}"))
        finally:
            process.wait()
            self.events.put(("exit", kind, index, process.returncode, process))

    def _process_events(self):
        """Apply child output and process exits on the Tk main thread."""
        while True:
            try:
                event = self.events.get_nowait()
            except queue.Empty:
                break
            if event[0] == "line":
                _, kind, index, line = event
                self._handle_line(kind, index, line)
            elif event[0] == "rtt":
                _, index, token, action, *data = event
                if token == self.client_probe_tokens[index]:
                    if action == "candidate":
                        self.client_test_edges[index] = data[0]
                        self.draw_graph()
                    elif action == "measured":
                        self.client_probe_rtts[index][data[0]] = data[1]
                        self.draw_graph()
                    elif action == "selected":
                        self.client_edges[index] = data[0]
                        self.draw_graph()
                    elif action == "log":
                        self._append_log(data[0])
                    elif action == "finished":
                        self.client_test_edges.pop(index, None)
                        best_port = data[0]
                        if best_port is None:
                            self._append_log(
                                f"[RTT] Client {index}: no server replied to an echo check."
                            )
                        else:
                            self._append_log(
                                f"[RTT] Client {index}: checks complete; "
                                f"best server is {best_port}."
                            )
                        self.draw_graph()
            else:
                _, kind, index, return_code, process = event
                processes = self.server_processes if kind == "server" else self.client_processes
                if processes[index] is process:
                    processes[index] = None
                    statuses = self.server_status if kind == "server" else self.client_status
                    statuses[index] = f"Exited ({return_code})"
                    if kind == "server":
                        port = int(self.server_port_vars[index].get())
                        self.network_connected_ports.discard(port)
                        self.server_edges = {
                            edge for edge in self.server_edges if port not in edge
                        }
                    else:
                        self._cancel_client_rtt_checks(index)
                        self.client_probe_tokens[index] += 1
                        self.client_test_edges.pop(index, None)
                        self.client_probe_rtts[index].clear()
                        self.client_edges.pop(index, None)
                    self._set_button(kind, index, "Start")
                    self.draw_graph()
        self.root.after(100, self._process_events)

    def _handle_line(self, kind, index, line):
        """Update connection edges from a server or client log line."""
        self._append_log(f"[{kind.title()} {index}] {line}")
        if kind == "server":
            own_port = int(self.server_port_vars[index].get())
            match = re.search(r"Connected to server (\d+)", line)
            if not match:
                match = re.search(r"Connected to (?:known|bootstrap) server [^:]+:(\d+)", line)
            if match:
                peer_port = int(match.group(1))
                if peer_port in PORTS:
                    if peer_port != own_port:
                        self.server_edges.add(tuple(sorted((own_port, peer_port))))
                    self.server_status[index] = "Running"
                    self._queue_server_for_clients(peer_port)
                    self._queue_server_for_clients(own_port)
                    self.draw_graph()
            elif "[LISTENING]" in line:
                self.server_status[index] = "Listening"
                self.draw_graph()
        else:
            match = re.search(r"\[CONNECTED\]\s+([^:]+):(\d+)\s+as\s+(.+)\.", line)
            if match:
                host, port, name = match.groups()
                self.client_name_vars[index].set(name)
                self.client_status[index] = f"Connected to {host}:{port}"
                self.draw_graph()

    def _set_button(self, kind, index, text):
        """Set a server or client button back to the requested action."""
        buttons = self.server_buttons if kind == "server" else self.client_buttons
        command = (
            (lambda i=index: self.start_server(i))
            if kind == "server"
            else (lambda i=index: self.start_client(i))
        )
        buttons[index].configure(text=text, command=command)

    def _append_log(self, line):
        """Append one message to the activity panel."""
        self.log.configure(state="normal")
        self.log.insert("end", line + os.linesep)
        self.log.see("end")
        self.log.configure(state="disabled")

    def draw_graph(self):
        """Draw server/client nodes and the connections currently observed."""
        if not hasattr(self, "canvas"):
            return
        self.canvas.delete("all")
        width = max(self.canvas.winfo_width(), 500)
        height = max(self.canvas.winfo_height(), 400)
        server_positions = {}
        server_heights = (0.18, 0.31, 0.23, 0.37, 0.20)
        for index, port_var in enumerate(self.server_port_vars):
            try:
                port = int(port_var.get())
            except ValueError:
                port = PORTS[index]
            server_positions[port] = (
                width * (index + 1) / (len(PORTS) + 1),
                height * server_heights[index],
            )
        client_positions = {
            index: (width * (index + 1) / (len(PORTS) + 1), height * 0.74)
            for index in range(len(PORTS))
        }

        for server_a, server_b in self.server_edges:
            if server_a in server_positions and server_b in server_positions:
                self.canvas.create_line(
                    *server_positions[server_a],
                    *server_positions[server_b],
                    fill="#64748b",
                    width=2,
                )
        for index, port in self.client_edges.items():
            if port in server_positions:
                self.canvas.create_line(
                    *client_positions[index],
                    *server_positions[port],
                    fill="#16a34a",
                    width=2,
                )
        for index, port in self.client_test_edges.items():
            if port in server_positions:
                self.canvas.create_line(
                    *client_positions[index],
                    *server_positions[port],
                    fill="#9333ea",
                    width=3,
                    dash=(6, 4),
                )
        for index, port_var in enumerate(self.server_port_vars):
            try:
                port = int(port_var.get())
            except ValueError:
                port = PORTS[index]
            x, y = server_positions[port]
            color = "#dbeafe" if self.server_status[index] in ("Running", "Listening") else "#f1f5f9"
            self._draw_node(x, y, f"S{index}\n{port}", color)

        for index, (x, y) in client_positions.items():
            name = self.client_name_vars[index].get().strip() or f"Client {index}"
            color = "#dcfce7" if index in self.client_edges else "#f1f5f9"
            self._draw_node(x, y, name, color)

        for index in range(len(PORTS)):
            visible_ports = set()
            if index in self.client_edges:
                visible_ports.add(self.client_edges[index])
            if index in self.client_test_edges:
                visible_ports.add(self.client_test_edges[index])
            for port in visible_ports:
                if port not in server_positions:
                    continue
                client_x, client_y = client_positions[index]
                server_x, server_y = server_positions[port]
                rtt = self.client_probe_rtts[index].get(port)
                label = f"{rtt * 1000:.2f} ms" if rtt is not None else "testing..."
                self._draw_edge_label(
                    (client_x + server_x) / 2,
                    (client_y + server_y) / 2 - 12,
                    label,
                )

    def _draw_edge_label(self, x, y, label):
        """Draw a readable RTT label over a client-server edge."""
        text_id = self.canvas.create_text(
            x,
            y,
            text=label,
            fill="#0f172a",
            font=("", 9, "bold"),
        )
        bounds = self.canvas.bbox(text_id)
        if bounds:
            left, top, right, bottom = bounds
            background = self.canvas.create_rectangle(
                left - 3,
                top - 1,
                right + 3,
                bottom + 1,
                fill="white",
                outline="white",
            )
            self.canvas.tag_lower(background, text_id)

    def _draw_node(self, x, y, label, color):
        """Draw a labeled circle on the graph."""
        radius = 38
        self.canvas.create_oval(
            x - radius,
            y - radius,
            x + radius,
            y + radius,
            fill=color,
            outline="#334155",
            width=2,
        )
        self.canvas.create_text(x, y, text=label, width=radius * 1.8, justify="center")

    def _close(self):
        """Stop child processes before closing the window."""
        for index in range(len(PORTS)):
            self._cancel_client_rtt_checks(index)
        for kind, processes in (
            ("server", self.server_processes),
            ("client", self.client_processes),
        ):
            for index, process in enumerate(processes):
                if process is not None:
                    if kind == "client":
                        try:
                            process.stdin.write("exit\n")
                            process.stdin.flush()
                        except OSError:
                            pass
                    process.terminate()
        self.root.destroy()


def main():
    """Open the network visualizer window."""
    root = tk.Tk()
    NetworkWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()