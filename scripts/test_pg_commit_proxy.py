import socket
import struct
import unittest
from pg_commit_proxy import frame, commit, Proxy, Handler


class Frames(unittest.TestCase):
    def test_once_fault_is_shared_across_connections(self):
        with Proxy(("127.0.0.1", 0), Handler) as proxy:
            proxy.once = True
            self.assertTrue(proxy.claim_fault())
            self.assertFalse(proxy.claim_fault())
            proxy.once = False
            self.assertTrue(proxy.claim_fault())

    def test_startup_and_command(self):
        left, right = socket.socketpair()
        with left, right:
            startup = struct.pack('!II', 8, 196608)
            left.sendall(startup)
            self.assertEqual(frame(right, startup=True), (b'', struct.pack('!I', 196608), startup))
            message = b'Q' + struct.pack('!I', 11) + b'COMMIT\0'
            left.sendall(message)
            kind, body, copied = frame(right)
            self.assertTrue(commit(kind, body))
            self.assertEqual(copied, message)
            self.assertFalse(commit(b'Q', b'ROLLBACK\0'))
            self.assertFalse(commit(b'C', b'COMMIT\0'))

    def test_invalid_length_and_eof(self):
        for length in (0, 3, 16 * 1024 * 1024 + 1):
            left, right = socket.socketpair()
            with left, right:
                left.sendall(b'Q' + struct.pack('!I', length))
                with self.assertRaises(ValueError):
                    frame(right)
        left, right = socket.socketpair()
        left.close()
        with right, self.assertRaises(EOFError):
            frame(right)
