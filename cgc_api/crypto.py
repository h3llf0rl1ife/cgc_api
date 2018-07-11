import hashlib
import binascii
import hmac
import base64
import json
import secrets

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend


class Crypto:
    def __init__(self, secret):
        self.secret = bytearray(secret, 'utf-8')
        self.secret_key = hashlib.md5(self.secret).digest()

    def b64decode(self, b64_input):
        return base64.b64decode(b64_input)

    def hexlify(self, byte_input):
        return binascii.hexlify(byte_input)

    def unhexlify(self, byte_input):
        return binascii.unhexlify(byte_input)

    def hashString(self, algorithm, msg, key=None, salt=None):
        if key:
            return hmac.new(key, msg, algorithm).digest()

        elif salt:
            return hashlib.pbkdf2_hmac(algorithm, msg, salt, 100000)

        return hashlib.new(algorithm, msg).digest()

    def checkHashString(self, byte_input_a, byte_input_b):
        return hmac.compare_digest(byte_input_a, byte_input_b)

    def generateToken(self, nbytes):
        return secrets.token_hex(nbytes)

    def b64encode(self, b64_input):
        return base64.b64encode(b64_input)

    def writeJWT(self, header, payload):
        header_json = json.dumps(header)
        header_b64 = self.b64encode(
            bytearray(header_json, 'utf-8')).decode('utf-8')

        payload_json = "pppppppppppp" + json.dumps(payload)
        payload_b64 = self.b64encode(bytearray(payload_json, 'utf-8'))

        padder = padding.PKCS7(128).padder()
        padded_payload = padder.update(payload_b64) + padder.finalize()

        payload_iv = secrets.token_bytes(16)

        cipher = Cipher(
            algorithms.AES(self.secret_key), modes.CBC(payload_iv),
            backend=default_backend())
        encryptor = cipher.encryptor()

        payload_byte = encryptor.update(padded_payload) + encryptor.finalize()
        payload_b64 = self.b64encode(payload_byte).decode('utf-8')

        header_plus_payload = '.'.join((header_b64, payload_b64))
        signature = self.hashString(
            'sha512', bytearray(header_plus_payload, 'utf-8'), self.secret_key)
        signature_b64 = self.b64encode(signature).decode('utf-8')

        jwt = '.'.join((header_b64, payload_b64, signature_b64))

        return jwt
    
    def readJWT(self, jwt):
        splitToken = jwt.split('.')

        header_b64 = bytearray(splitToken[0], 'utf-8')
        header_json = self.b64decode(header_b64)
        header = json.loads(header_json)

        payload_b64 = bytearray(splitToken[1], 'utf-8')
        payload_aes = self.b64decode(payload_b64)

        signature_b64 = bytearray(splitToken[2], 'utf-8')
        signature = self.b64decode(signature_b64)

        header_plus_payload = '.'.join(splitToken[:2])
        expected_signature = self.hashString(
            'sha512', bytearray(header_plus_payload, 'utf-8'), self.secret_key)

        signature_check = self.checkHashString(
            signature, expected_signature)

        if signature_check:
            payload_iv = payload_aes[0:16]
            payload_data = payload_aes[16:]

            cipher = Cipher(
                algorithms.AES(self.secret_key), modes.CBC(payload_iv),
                backend=default_backend())
            decryptor = cipher.decryptor()

            payload_byte = decryptor.update(payload_data)
            payload_byte += decryptor.finalize()

            payload_json = self.b64decode(payload_byte)
            payload = json.loads(payload_json)

            return header, payload

        return None, None
