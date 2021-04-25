import base64
import hashlib

from Crypto import Random
from Crypto.Cipher import AES

class AESCipher:
    def __init__(self, secretKey):
        self.BS = 16
        self.pad = lambda s: s+(self.BS - len(s)%self.BS)*chr(self.BS-len(s)%self.BS)
        self.unpad = lambda s: s[0:-s[-1]]
        self.key = hashlib.sha256(secretKey.encode('utf-8')).digest()

    def encrypt(self, raw):
        raw = self.pad(raw).encode('utf-8')
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv+ cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self.unpad(cipher.decrypt(enc[16:]))

    def encrypt_str(self, raw):
        return self.encrypt(raw).decode('utf-8')

    def decrypt_str(self, enc):
        if type(enc) == str:
            enc = str.encode(enc)
        return self.decrypt(enc).decode('utf-8')

if __name__ == "__main__":
    a = 0

    fkey = open("security_key", 'r')
    security_key = fkey.readline()
    fkey.close()
    aesCipher = AESCipher(security_key)
    encrypt_id = aesCipher.encrypt_str("crexy")
    encrypt_pwd = aesCipher.encrypt_str("lowstar9130!")

    f = open("databaseInfo.dat", 'w')
    f.write(encrypt_id+"\n")
    f.write(encrypt_pwd + "\n")
    f.close()
    # encrypt = aesCipher.encrypt_str("Kang")
    # print(encrypt)
    # decrypt = aesCipher.decrypt_str(encrypt)
    # print(decrypt)