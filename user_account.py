import bcrypt

class UserAccount():
    def __init__(self, USER_CLT):
        self.USER_CLT = USER_CLT

    def addUserAccount(self, user_id, password, email):
        # bcrypt hash transfer
        password = (bcrypt.hashpw(password.encode('UTF-8'), bcrypt.gensalt())).decode('utf-8')
        self.USER_CLT.insert_one({'user_id': user_id, 'password': password, 'email': email})

    # 로그인 체크
    def login_check(self, user_id, password):
        # bcrypt hash transfer
        password = password.encode('utf-8')
        # DB에 해당 계정 정보가 있는지 확인
        account = self.USER_CLT.find_one({'user_id':user_id})
        # 값이 유무 확인 결과값 account 변수로 넣기
        check_password = bcrypt.checkpw(password, account['password'].encode('utf-8'))
        return account, check_password