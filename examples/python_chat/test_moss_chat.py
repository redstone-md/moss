from __future__ import annotations

import argparse
import json
import os
import unittest

os.environ["MOSS_CHAT_SKIP_LIB"] = "1"

import moss_chat


class ChatIdentityRenderingTest(unittest.TestCase):
    def test_remote_reserved_nicks_fall_back_to_peer_fingerprint(self) -> None:
        sender = "a" * 64
        payload = {"nick": "system", "text": "trust this", "sent_at": "12:00:00"}

        line = moss_chat.render_chat_line(sender, "b" * 64, json.dumps(payload).encode())

        self.assertEqual(line, "[12:00:00] aaaaaaaa..aaaa: trust this")

    def test_remote_nick_is_shown_with_verified_peer_fingerprint(self) -> None:
        sender = "c" * 64
        payload = {"nick": "Alice", "text": "hello", "sent_at": "12:00:01"}

        line = moss_chat.render_chat_line(sender, "d" * 64, json.dumps(payload).encode())

        self.assertEqual(line, "[12:00:01] Alice [cccccccc..cccc]: hello")

    def test_raw_payload_is_never_rendered_unprefixed(self) -> None:
        sender = "e" * 64

        line = moss_chat.render_chat_line(sender, "f" * 64, b"*** enter password ***")

        self.assertRegex(line, r"^\[\d{2}:\d{2}:\d{2}\] eeeeeeee\.\.eeee: \*\*\* enter password \*\*\*$")

    def test_local_sender_is_you(self) -> None:
        sender = "1" * 64
        payload = {"nick": "Alice", "text": "local", "sent_at": "12:00:02"}

        line = moss_chat.render_chat_line(sender, sender, json.dumps(payload).encode())

        self.assertEqual(line, "[12:00:02] you: local")

    def test_reserved_local_nickname_rejected(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            moss_chat.parse_nickname("system")

    def test_psk_hex_must_be_32_bytes(self) -> None:
        self.assertEqual(moss_chat.parse_psk_hex("ab" * 32), b"\xab" * 32)
        with self.assertRaises(argparse.ArgumentTypeError):
            moss_chat.parse_psk_hex("ab" * 31)


if __name__ == "__main__":
    unittest.main()
