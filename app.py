import re

from flask import Flask, render_template, url_for, request, session, redirect, jsonify
from flask_wtf import csrf
from pymongo import MongoClient
from user_account import UserAccount
from flask_wtf.csrf import CSRFProtect

mongoClient = MongoClient('mongodb://{0}:{1}@192.168.219.107'.format("crexy", "lowstar9130!"))
database = mongoClient.Stock_Investment




class MyFlask(Flask):

  jinja_options = Flask.jinja_options.copy()
  jinja_options.update(dict(
    block_start_string='{%',
    block_end_string='%}',
    variable_start_string='((', #vue 변수사용으로 jinja 변수규칙을 ((변수))로 변경
    variable_end_string='))',
    comment_start_string='{#',
    comment_end_string='#}',
  ))

#사용자 추가


#addUserAccount('lowstar9', 'crexy9130!', 'lowstar9@gmail.com')
#addUserAccount('p5027002', 'pinari159/h', '5027002@naver.com')

app = MyFlask(__name__)

@app.route('/')
def index():
    app.logger.info('index()')
    session['login'] = True
    if 'login' in session : # session 에 로그인 키 존재 여부 검사
        if session['login'] == False: # 로그인 상태가 아니라면
            return redirect(url_for('login')) # 로그인 화면으로
    else: # 로그인 키가 없다면 => login 화면으로
        return redirect(url_for('login'))

    return render_template('index.html')

@app.route('/login')
def login():
    app.logger.info('login()')
    return render_template('login.html')

@app.route('/try_login', methods=['POST'])
def try_login():
    app.logger.info('try_login()')

    user_id = request.form.get('user_id')
    password = request.form.get('password')
    # 계정 검사

    # 사용자 계정 객체
    user_account = UserAccount(database["USER_INFO"])

    try:
        account, loginOk = user_account.login_check(user_id, password)
        if loginOk == True:
            # Session
            session['login'] = True
    except:
        return "Login Fail"

    return redirect(url_for('index'))

# 종목 S-RIM데이터 조회

@app.route('/stock_srim', methods=['GET', 'POST'])
def stock_srim():
    app.logger.info('stock_srim()')
    #keyword = request.args.get('keyword')
    data = request.get_json()
    #data = request.args.get("keyword")
    #data = request.args.get_json();
    keyword = data["keyword"]
    app.logger.info(keyword)

    # S-RIM 값 조회 쿼리
    CROP_CLT = database["STOCK_CROP_DATA_CLT"] #종목정보 컬렉션(테이블)

    rgx = re.compile(f'.*{keyword}.*', re.IGNORECASE)  # compile the regex
    #rsltList = CROP_CLT.find({{'$or':[{'stock_code':rgx},{'stock_name':rgx}]},
    #               {'_id':0, 'stock_code':1, 'stock_name':1, 'cur_price':1, 'S-RIM.080':1, 'S-RIM.090':1, 'S-RIM.100':1,}})

    rsltList = CROP_CLT.find({'$or':[{'stock_code':rgx},{'stock_name':rgx}]})

    srimList = []
    for doc in rsltList:
        dict={}
        dict['stock_code'] = doc['stock_code']
        dict['stock_name'] = doc['stock_name']
        dict['price'] = doc['cur_price']
        if 'S-RIM' in doc:
            dict['srim80'] = doc['S-RIM']['080']
            dict['srim90'] = doc['S-RIM']['090']
            dict['srim100'] = doc['S-RIM']['100']
        else:
            dict['srim80'] = 0
            dict['srim90'] = 0
            dict['srim100'] = 0

        srimList.append(dict)

    return jsonify(srimList)

if __name__ == "__main__":
    app.config['SECRET_KEY'] = 'rkdworbsmswkdbfmfwodcnlgksek'
    csrf = CSRFProtect()
    csrf.init_app(app)
    app.run(debug=True)