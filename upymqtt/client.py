import time
import asyncio


class FallbackLogger:
    def info(self, msg):
        print(f"INFO: {msg}")

    def error(self, msg):
        print(f"ERROR: {msg}")

    def debug(self, msg):
        print(f"DEBUG: {msg}")


class MQTTLoginError(RuntimeError):
    pass


class MQTTClient:
    PROTOCOL_LEVEL = 4  # MQTT v3.1.1
    PROTOCOL_NAME = "MQTT"
    CLEAN_SESSION = 0x02
    USERNAME_FLAG = 0x80
    PASSWORD_FLAG = 0x40

    PACKET_CONNECT_ACK = 0x20
    PACKET_CONNECT = 0x10

    def __init__(
        self,
        client_id,
        server,
        port=0,
        user=None,
        password=None,
        keepalive=20,
        ping_interval=5,
        ssl=False,
        ssl_params={},
    ):
        """
        Create a new MQTT client instance.

        :param client_id: the unique client id string used when connecting to the server
        :param server: the server to connect to
        :param port: the port to connect to the server on
        :param user: the username to authenticate with
        :param password: the password to authenticate with
        :param keepalive: the maximum period in seconds allowed between communications with the broker
        :param ssl: a boolean flag to enable SSL/TLS
        """
        self._client_id = client_id
        self._server = server
        self._port = port
        self._user = user
        self._password = password
        self._keepalive = keepalive
        self._ping_interval = ping_interval
        self._ssl = ssl
        self._ssl_params = ssl_params
        self._sock = None
        self._msgid = 1
        self._last_msg = time.time()
        self._reader = None
        self._writer = None
        self._logger = FallbackLogger()
        self._is_running = False
        self._auth_event = asyncio.Event()
        self._logged_in = False
        self._exc = None
        self._read_task = None
        self._ping_task = None

    @property
    def is_running(self):
        return self._is_running

    async def disconnect(self):
        """
        Disconnect from the server.
        """
        self._logger.info("Disconnecting")
        self._is_running = False
        self._read_task.cancel()
        self._ping_task.cancel()

        self._writer.close()
        await self._writer.wait_closed()

        self._logger.info("Disconnected")

    async def connect(self, clean_session=True):
        """
        Connect to the server.

        :param clean_session: a boolean flag that determines the client type
        """
        if self._is_running:
            return

        # Cancel any existing tasks from previous connections
        if self._ping_task:
            self._ping_task.cancel()
        
        if self._read_task:
            self._read_task.cancel()

        self._logger.info(f"Connecting to MQTT broker {self._server}:{self._port}")
        self._auth_event.clear()
        self._exc = None

        self._reader, self._writer = await asyncio.open_connection(
            host=self._server, port=self._port
        )
        # Send CONNECT packet
        connect_packet = self._build_connect_packet(clean_session=clean_session)
        if not await self._write(connect_packet):
            self._logger.error("Error sending CONNECT")
        else:
            self._logger.info(f"Connected to MQTT broker {self._server}:{self._port}")
            self._is_running = True

        # Start read loop
        self._read_task = asyncio.create_task(self._read_loop())
        
        # Wait for auth response
        await self._auth_event.wait()
        if not self._logged_in:
            raise MQTTLoginError(f"Login failed with MQTT broker {self._server}:{self._port}")
        
        self._logger.info(f"Logged in to MQTT broker {self._server}:{self._port}")
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def publish(self, topic, message, qos=0, retain=False):
        """
        Publish a message to the server.

        :param topic: the topic to publish to
        :param message: the message to publish
        :param qos: the quality of service level to use
        :param retain: a boolean flag that determines if the message should be retained
        """
        await self.connect()
        msg_id = self._msgid = (self._msgid + 1) % 65536 if qos else 0
        packet = b''.join([
            bytes([(0x30 | (qos << 1) | retain)]),
            self._encode_remaining_length(2 + len(topic) + (2 if qos else 0) + len(message)),
            len(topic).to_bytes(2, 'big'),
            topic.encode(),
            msg_id.to_bytes(2, 'big') if qos else b'',
            message if isinstance(message, bytes) else str(message).encode()
        ])
        sent = await self._write(packet)
        return None if not sent else msg_id
    
    async def _write(self, packet):
        try:
            self._writer.write(packet)
            await self._writer.drain()
            return True
        except OSError as e:
            self._logger.error(f"Error sending packet: {e}")
            self._is_running = False
            return False

    def _build_connect_packet(self, clean_session=True):
        """Build MQTT CONNECT packet"""
        flags = (
            (self.USERNAME_FLAG if self._user else 0)
            | (self.PASSWORD_FLAG if self._password else 0)
            | (self.CLEAN_SESSION if clean_session else 0)
        )

        packet = b"".join(
            [
                self._encode_string(self.PROTOCOL_NAME),  # Protocol name
                bytes([self.PROTOCOL_LEVEL]),  # Protocol level
                bytes([flags]),  # Connect flags
                self._keepalive.to_bytes(2, "big"),  # Keepalive
                self._encode_string(self._client_id),  # Client ID
                self._encode_string(self._user or ""),  # Username
                self._encode_string(self._password or ""),  # Password
            ]
        )

        return bytes([MQTTClient.PACKET_CONNECT]) + self._encode_remaining_length(len(packet)) + packet

    async def _handle_packet(self, packet_type, packet):
        """
        Handle incoming messages from the server.

        :param message: the message to handle
        """
        if packet_type == MQTTClient.PACKET_CONNECT_ACK:
            self._logged_in = (packet[1] == 0)
            self._auth_event.set()

    async def _read_loop(self):
        while self._is_running:
            try:
                fixed_header_buff = await self._reader.read(1)
                if not fixed_header_buff:
                    self._is_running = False
                    self._logger.info("Server closed connection")
                    break
                fixed_header = fixed_header_buff[0]
                packet_len = await self._decode_remaining_length()
                packet = await self._reader.read(packet_len)
            except OSError as e:
                self._logger.error(f"Error reading data: {e}")
                self._is_running = False
            else:
                await self._handle_packet(fixed_header, packet)

    async def _ping_loop(self):
        while self._is_running:
            await asyncio.sleep(self._ping_interval)
            await self._write(b"\xc0\x00")

    def _encode_remaining_length(self, n: int) -> bytes:
        """Pack MQTT remaining length into bytes (128-base)"""
        return bytes(
            [
                (n >> i * 7) & 0x7F | (0x80 if n >= (1 << (i + 1) * 7) else 0)
                for i in range(4)
                if n >= (1 << i * 7)
            ]
        )

    async def _decode_remaining_length(self) -> int:
        """Read and decode MQTT remaining length from stream"""
        m, v = 1, 0
        while m <= 2097152:  # 128^3
            b = (await self._reader.read(1))[0]
            v += (b & 0x7F) * m
            m *= 128
            if not b & 0x80:
                return v
        raise ValueError("malformed length")

    def _encode_string(self, string):
        """Encode a string with length prefix"""
        return (
            (len(encoded := string.encode("utf-8")).to_bytes(2, "big") + encoded)
            if string
            else b"\x00\x00"
        )

