from flask import Flask, render_template, url_for, request, session, redirect, jsonify

from user_account import UserAccount
from flask_wtf.csrf import CSRFProtect
from StockDB import stockDB

from Service import haService # 서비스 객체


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


# tui-grid datasource format
def make_tui_ds_fmt(content, page, totCnt):
    return {
        "result": True,
        "data": {
            "contents": content,
            "pagination": {
                "page": page,
                "totalCount": totCnt
            }
        }
    }

def checkLogin():
    if 'login' in session : # session 에 로그인 키 존재 여부 검사
        if session['login'] == False: # 로그인 상태가 아니라면
            return False # 로그인 화면으로
    else: # 로그인 키가 없다면 => login 화면으로
        return False
    return True

@app.route('/')
def index():
    app.logger.info('index()')
    session['login'] = True

    if checkLogin() == False:
        return redirect(url_for('login'))

    return render_template('index.html')

# 종목 리스트 페이지
@app.route('/stock_list')
def stock_list():
    app.logger.info('stock_list()')
    session['login'] = True

    if checkLogin() == False:
        return redirect(url_for('login'))

    return render_template('stock_list.html')

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
    user_account = UserAccount(stockDB.FS_DB["USER_INFO"])

    try:
        account, loginOk = user_account.login_check(user_id, password)
        if loginOk == True:
            # Session
            session['login'] = True
    except:
        return "Login Fail"

    return redirect(url_for('index'))


# 종목 S-RIM데이터 조회
# @app.route('/stock_srim', methods=['GET', 'POST'])
# def stock_srim():
#     app.logger.info('stock_srim()')
#     #keyword = request.args.get('keyword')
#     data = request.get_json()
#     #data = request.args.get("keyword")
#     #data = request.args.get_json();
#     keyword = data["keyword"]
#     app.logger.info(keyword)
#     return jsonify(srimList)



# 종목 정보 조회
@app.route('/stock_list/search', methods=['POST', 'GET'])
def stock_list_search():
    app.logger.info('stock_srim()')

    #searchOpt = request.form.get("data")  # request.form으로 POST 데이터 수신
    searchOpt = request.get_json()
    #getdata = request.args.get("stock_keyword")
    #searchOpt = request.form.get("data") # request.form으로 POST 데이터 수신
    #page = request.form.get("page")  # request.form으로 POST 데이터 수신

    stock_keyword = None
    business_Keyword = None
    customer_Keyword = None
    product_Keyword = None
    page = 1
    perPage = 10

    if searchOpt != None:
        if "page" in searchOpt:
            page = searchOpt["page"]
        if "perPage" in searchOpt:
            perPage = searchOpt["perPage"]
        if "stock_keyword" in searchOpt:
            stock_keyword = searchOpt["stock_keyword"]
        if "business_Keyword" in searchOpt:
            business_Keyword = searchOpt["business_Keyword"]
        if "customer_Keyword" in searchOpt:
            customer_Keyword = searchOpt["customer_Keyword"]
        if "product_Keyword" in searchOpt:
            product_Keyword = searchOpt["product_Keyword"]

    list, totCnt = haService.searchStockInfoList(stock_keyword, business_Keyword, customer_Keyword, product_Keyword)

    return make_tui_ds_fmt(list, page, totCnt)



# 종목 정보 조회
@app.route('/stock_list/modify', methods=['POST', 'GET'])
def stock_info_modify():
    stockInf = request.get_json()
    #stockInf = request.form.get("data") # request.form으로 POST 데이터 수신
    modifyCnt = haService.modifyStockBusinessInfo(stockInf)
    if modifyCnt == 0: return 0
    return stockInf

if __name__ == "__main__":
    app.config['SECRET_KEY'] = 'rkdworbsmswkdbfmfwodcnlgksek'
    csrf = CSRFProtect()
    csrf.init_app(app)
    app.run(host='0.0.0.0', debug=True, port=5050)