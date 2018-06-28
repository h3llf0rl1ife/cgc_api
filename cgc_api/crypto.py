import hashlib
import binascii
import hmac
import base64
import json

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


class Crypto:
    def __init__(self, secret):
        self.secret = bytearray(secret, 'utf-8')
        self.secret_key = hashlib.md5(self.secret).digest()

    def readJWT(self, jwt):
        splitToken = jwt.split('.')

        # header_b64 = bytearray(splitToken[0], 'utf-8')
        # header_json = self.b64decode(header_b64)

        payload_b64 = bytearray(splitToken[1], 'ansi')
        payload_aes = self.b64decode(payload_b64)

        signature_b64 = bytearray(splitToken[2], 'ansi')
        signature = self.b64decode(signature_b64)

        header_plus_payload = '.'.join(splitToken[:2])
        expected_signature = self.hashString(
            'sha256', bytearray(header_plus_payload, 'utf-8'), self.secret_key)

        signature_challenge = self.checkHashString(
                                    signature, expected_signature)

        if signature_challenge:
            payload_iv = payload_aes[0:16]
            payload_data = payload_aes[16:]

            cipher = Cipher(
                algorithms.AES(self.secret_key), modes.CBC(payload_iv),
                backend=default_backend())
            decryptor = cipher.decryptor()

            payload_byte = decryptor.update(payload_data)
            payload_json = self.b64decode(payload_byte)

            return json.loads(payload_json)

        else:
            return None

    def b64decode(self, b64_input):
        return base64.b64decode(b64_input)

    def hexlify(self, byte_input):
        return binascii.hexlify(byte_input)

    def unhexlify(self, byte_input):
        return binascii.unhexlify(byte_input)

    def hashString(self, algorithm, msg, key=None):
        if key:
            hmac_function = hmac.new(key, msg, algorithm)
            return hmac_function.digest()

        hash_function = hashlib.new(algorithm)
        hash_function.update(msg)

        return hash_function.digest()

    def checkHashString(self, byte_input_a, byte_input_b):
        return hmac.compare_digest(byte_input_a, byte_input_b)
