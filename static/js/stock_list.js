let stockInfo_gird = null; // 종목 정보 그리드
let infModifyMode = false; // 종목정보 수정모드
let selectedRowKey = -1    // 현재 선택된 그리드 rowKey

// 종목S-RIM정보 찾기
function searchStockSRimInfo(){


    var data={
        stock_keyword:$('#search_box').val(),
        //param:Math.random()
    };

    stockInfo_gird.readData(1, data, true);
    //stockInfo_gird.readData(1, data, true);

    //ajax("/stock_srim", 'POST', JSON.stringify(data), printStockValueInfo);
}

// 그리드 생성
function initGrid(){

    var csrftoken = $('meta[name=csrf-token]').attr('content')

    // 그리드 데이터 소스 설정
    const dataSource = {
      api: {
        readData: { url: '/stock_list/search', method: 'POST' }
      },
      contentType: 'application/json',
      headers: { 'X-CSRFToken': csrftoken }, //datasource request 헤더에 csrf 토큰 정보를 설정!!
      serializer(params) {
        return Qs.stringify(params);
      }
    }

    // S-RIM 컬럼 포멧터
    function srimColumnFmt({ row, column, value }) {
        var ratio = 0;
        var color = "black";
        if(value != 0){
            ratio = row.cur_price/value*100
            ratio = decimal2p(ratio)
            color = "#00db3a";
            if(ratio == 100)
                color = "pink";
            else if(ratio > 100)
                color = "gray"

            value = num3Comma(value)
            var rsltFmt = "<p style='color:"+color+"'>"+value+"("+ratio+"%)</p>"
            return rsltFmt;
        }
        return "-";
    }

    stockInfo_gird = new tui.Grid({
      el: document.getElementById('grid'),
      rowHeaders: ['rowNum'],
      initialRequest: false, // set to true by default
      data:dataSource,
      scrollX: true,
      scrollY: true,
      bodyHeight:350,
      pageOptions: {
        perPage: 10
      },
      //pagination:true,
      columns: [
        {
          header: '종목명',
          name: 'stock_name',
          width:200
        },
        {
          header: '종목코드',
          name: 'stock_code',
          align: 'center',
          width:100
        },
        {
          header: '현재가(전일대비)',
          name: 'cur_price',
          align: 'center',
          width:150,
          formatter: function({ row, column, value }) {

            var ratio = (value - row.last_price)/row.last_price*100
            ratio = decimal2p(ratio)
            var color = "red";
            if(ratio == 0)
                color = "black";
            else if(ratio < 0)
                color = "blue"
            value = num3Comma(value);
            var rsltFmt = "<p style='color:"+color+"'>"+value+"("+ratio+"%)</p>"
            return rsltFmt;
          }
        },
        {
          header: 'SRIM 80%(현가격 비율)',
          name: 'srim80',
          align: 'center',
          width:150,
          formatter: srimColumnFmt
        },
        {
          header: 'SRIM 90%(현가격 비율)',
          name: 'srim90',
          align: 'center',
          width:150,
          formatter: srimColumnFmt
        },

        {
          header: 'SRIM 100%(현가격 비율)',
          name: 'srim100',
          align: 'center',
          width:150,
          formatter: srimColumnFmt
        },
        {
          header: '거래량(전일대비, 유동주식수대비비율)',
          name: 'cur_volumn',
          align: 'center',
          width:230,
          formatter:function({ row, column, value }) {

            // 전일 대비 거래량
            var last_ratio = (value - row.last_volumn)/row.last_volumn*100
            last_ratio = decimal2p(last_ratio)
            var float_ratio = value/row.floatStocks*100
            float_ratio = decimal2p(float_ratio)

            var color = "red";
            if(last_ratio == 0)
                color = "black";
            else if(last_ratio < 0)
                color = "blue"
            value = num3Comma(value)
            var rsltFmt = "<p style='color:"+color+"'>"+value+"("+last_ratio+"%,"+float_ratio+"%)</p>"
            return rsltFmt;
          }
        },
        {
          header: '매출액',
          name: 'sales',
          align: 'center',
          width:100,
          formatter:function({ row, column, value }){ return num3Comma(value); }
        },
        {
          header: '영업이익',
          name: 'operating_profit',
          align: 'center',
          width:100,
          formatter:function({ row, column, value }){ return num3Comma(value); }
        },
        {
          header: '순이익',
          name: 'net_profit',
          align: 'center',
          width:100,
          formatter:function({ row, column, value }){ return num3Comma(value); }
        },
        {
          header: 'PER',
          name: 'ROE',
          align: 'center',
          formatter:function({ row, column, value }){ return decimal2p(value); }
        },
        {
          header: 'ROE',
          name: 'ROE',
          align: 'center',
          formatter:function({ row, column, value }){ return decimal2p(value); }
        },
        {
          header: '시가총액(억)',
          name: 'market_cap',
          width:100,
          align: 'right',
          formatter:function({ row, column, value }){
            value /= 100000000;
            value = decimal2p(value);
            return num3Comma(value);
           }
        }
      ]
    });

    function dispStockInfo(rowKey){
        if(rowKey == -1) return;
        row = stockInfo_gird.getRow(rowKey)
        $('#holdingCompany').val(row.holdingCompany);
        $('#customer').val(row.customer);
        $('#product').val(row.product);
        $('#business').val(row.business);
    }

    // 그리드 셀 포커스 변경 이벤트
    stockInfo_gird.on('focusChange', function(e){

        if(infModifyMode) return;   // 데이터 수정 모드에서는 기능 작동하지 않음

        console.log(e.rowKey);

        // 종목정보 전시
        dispStockInfo(e.rowKey);

        selectedRowKey = e.rowKey;

    });
}

$(function(){
    // CSRF 보안처리를 위한 AJAX 통신 Header 설정
    INIT_CSRF_REQUEST_HEADER();

    // 그리드 생성
    initGrid();

/*    stockInfo_gird.on('beforeRequest', function(data) {
      // Before sending the request
    }).on('response', function(data) {
      // When a response has been received regardless of success.
    }).on('successResponse', function(data) {
      // When the result is set to true
    }).on('failResponse', function(data) {
      // When the result is set to false
    }).on('errorResponse', function(data) {
      // When an error occurs
    });*/

    // ====================== UI Event ======================
    // 찾기 버튼 이벤트
    $('#btn_search').on('click', function(e){
        e.preventDefault();
        searchStockSRimInfo();
    });

    $('#search_box').on('keydown', function(e){
        if(e.keyCode == 13)
            searchStockSRimInfo();
    });

    function setInputComponentState(modifyMode){
        $('#holdingCompany').prop('readonly', !modifyMode);
        $('#customer').prop('readonly', !modifyMode);
        $('#product').prop('readonly', !modifyMode);
        $('#business').prop('readonly', !modifyMode);
        if(modifyMode)
            stockInfo_gird.disable();
        else
            stockInfo_gird.enable();
    }



    // 종목정보 작성 버튼
    $('#btn_write_stock_info').on('click', function(e){
        e.preventDefault();

        var btnText = infModifyMode?"종목 정보 작성":"작성 완료"
        $('#btn_write_stock_info').text(btnText);

        if(infModifyMode == false){ // 작성시작
            setInputComponentState(true); // 작성컨트롤 활성화
            $('#btn_cancel_stock_info').prop("disabled", true); // 작성취소 버튼 활성화
        }else{ // 작성완료
            setInputComponentState(false); // 작성컨트롤 비활성화
            $('#btn_cancel_stock_info').prop("disabled", true); // 작성취소 버튼 비활성화

            // 작성정보 서버로 전송
            var stock_code = stockInfo_gird.getValue(selectedRowKey, "stock_code");
            var holdingCompany = $('#holdingCompany').val();
            var customer = $('#customer').val();
            var product = $('#product').val();
            var business = $('#business').val();

            var data = {
                "stock_code":stock_code,
                "holdingCompany":holdingCompany,
                "customer":customer,
                "product":product,
                "business":business
            };

            ajax("/stock_list/modify", "POST", JSON.stringify(data), function(rslt){
                // 성공시 작성내용을 그리드에 적용해준다.
                if(rslt == 0){
                    alert("종목정보 작성 실패!");
                    return;
                }
                stockInfo_gird.setValue(selectedRowKey, "holdingCompany", rslt.holdingCompany);
                stockInfo_gird.setValue(selectedRowKey, "customer", rslt.customer);
                stockInfo_gird.setValue(selectedRowKey, "product", rslt.product);
                stockInfo_gird.setValue(selectedRowKey, "business", rslt.business);
            });

        }

        infModifyMode = !infModifyMode;

    });

    // 종목정보 작성취소 버튼
    $('#btn_cancel_stock_info').on('click', function(e){
        e.preventDefault();

        // 종목정보 전시 => 수정전의 종목전보를 전시
        dispStockInfo(selectedRowKey);

        $('#btn_write_stock_info').text("종목 정보 작성");
        setInputComponentState(false); // 작성컨트롤 비활성화
        infModifyMode = false;
    });
});

