import socket
import struct
import unittest
from pg_commit_proxy import frame, commit, Proxy, Handler, execution_marker


class Frames(unittest.TestCase):
    def test_once_fault_is_shared_across_connections(self):
        with Proxy(("127.0.0.1", 0), Handler) as proxy:
            proxy.once = True
            self.assertTrue(proxy.claim_fault())
            self.assertFalse(proxy.claim_fault())
            proxy.once = False
            self.assertTrue(proxy.claim_fault())

    def test_named_statement_arms_on_execution_not_preparation(self):
        observer=execution_marker(b'/* pgderive:epoch */')
        parse=b'epoch_statement\0/* pgderive:epoch */ SELECT 1\0\x00\x00'
        self.assertIsNone(observer.observe(b'P',parse))
        self.assertIsNone(observer.observe(b'B',b'epoch_portal\0epoch_statement\0'))
        self.assertTrue(observer.observe(b'E',b'epoch_portal\0\x00\x00\x00\x00'))
        self.assertIsNone(observer.observe(b'P',b'other\0SELECT 1\0\x00\x00'))
        self.assertIsNone(observer.observe(b'B',b'other_portal\0other\0'))
        self.assertFalse(observer.observe(b'E',b'other_portal\0\x00\x00\x00\x00'))
        self.assertTrue(observer.observe(b'E',b'epoch_portal\0\x00\x00\x00\x00'))
        observer.observe(b'C',b'Pepoch_portal\0')
        self.assertIsNone(observer.observe(b'E',b'epoch_portal\0\x00\x00\x00\x00'))
        self.assertTrue(observer.observe(b'Q',b'/* pgderive:epoch */ SELECT 1\0'))

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
