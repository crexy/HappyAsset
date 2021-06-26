let stockInfo_gird = null; // 종목 정보 그리드
let infModifyMode = false; // 종목정보 수정모드
let selectedRowKey = -1    // 현재 선택된 그리드 rowKey

// 종목S-RIM정보 찾기
function searchStocks(){
    var data={
        stock_keyword:$('#search_stock').val().trim(),
        holdingCompany_keyword:$('#search_holdingCompany').val().trim(),
        customer_keyword:$('#search_customer').val().trim(),
        product_keyword:$('#search_product').val().trim(),
        business_keyword:$('#search_business').val().trim(),
        subsidiaryCompany_keyword:$('#search_subsidiaryCompany').val().trim(),
        //param:Math.random()
    };

    stockInfo_gird.readData(1, data, true);
    //stockInfo_gird.readData(1, data, true);
    //ajax("/stock_srim", 'POST', JSON.stringify(data), printStockValueInfo);
}

// 종목 정보 전시
function dispStockInfo(rowKey){
    if(rowKey == -1) return;
    row = stockInfo_gird.getRow(rowKey)
    $('#stock_title').text("종목 정보("+row.stock_name+")")
    $('#holdingCompany').val(row.holdingCompany);
    $('#customer').val(row.customer);
    $('#product').val(row.product);
    $('#business').val(row.business);
    $('#subsidiaryCompany').val(row.subsidiaryCompany);
}

function priceUpDownColor(value){
    if(value > 50){
        return '#FF0000';
    }else if(value > 30){

    }else if(value > 10){

    }else if(value > 0){

    }else if(value < 50){

    }else if(value < 30){

    }else if(value < 10){

    }

    return '#000000';
}

// 가격 비율 컬러 포멧터
function priceRatioColorFmt({ row, column, value }) {
    value = decimal2p(value)
    var color = "red";
    if(value == 0)
        color = "black";
    else if(value < 0)
        color = "blue"
    value = decimal2p(value);
    var rsltFmt = "<p style='color:"+color+"'>"+value+"%</p>"
    return rsltFmt;
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
    function srim_ColumnFmt({ row, column, value }) {
        var color = "#00bf46";
        var srim_pr_col = column.name+"_PR"
        var srim_pr_val = row[srim_pr_col]

        if(srim_pr_val != 0){
            if(srim_pr_val == 100)
                color = "pink";
            else if(srim_pr_val > 100)
                color = "gray"
            value = num3Comma(value)
            var rsltFmt = "<p style='text-align:right;color:"+color+"'>"+value+"</p>"
            return rsltFmt;
        }
        return "-";
    }

    // S-RIM 가격대비 컬럼 포멧터
    function srimPR_ColumnFmt({ row, column, value }) {
        var color = "#00bf46";
        if(value != 0){
            if(value == 100)
                color = "pink";
            else if(value > 100)
                color = "gray"
            value = decimal2p(value)
            var rsltFmt = "<p style='color:"+color+"'>("+value+"%)</p>"
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
      bodyHeight:450,
      rowHeight:40,
      minRowHeight:20,
      minColumnWidth:150,
//      pageOptions: {
//        useClient: true,
//        perPage: 20
//      },
      columnOptions: { // 고정 컬럼 설정
        frozenCount: 3, // 3개의 컬럼을 고정하고
        frozenBorderWidth: 2 // 고정 컬럼의 경계선 너비를 2px로 한다.
      },

      //pagination:false,
      columns: [
        {
          header: '시장구분',
          name: 'market',
          align: 'center',
          width:100,
          resizable: true,
          filter: 'select'
        },
        {
          header: '종목명',
          name: 'stock_name',
          align: 'center',
          width:150,
          resizable: true,
          filter: {
            type: 'text',
            operator: 'OR',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '종목코드',
          name: 'stock_code',
          align: 'center',
          width:100,
          resizable: true,
          filter: {
            type: 'text',
            operator: 'OR',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '관련뉴스',
          name: 'news_count',
          align: 'center',
          width:100,
          resizable: true,
          sortable: true,
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '현재가',
          name: 'cur_price',
          align: 'right',
          width:100,
          sortable: true,
          resizable: true,
          align: 'center',
          formatter: function({ row, column, value }) {
            var df_rate = row.price_DR // 전일대비 등락률
            var color = "red";
            if(df_rate == 0)
                color = "black";
            else if(df_rate < 0)
                color = "blue"
            value = num3Comma(value);
            var rsltFmt = "<p style='text-align:right;color:"+color+"'>"+value+"</p>"
            return rsltFmt;
          }
        },
        {
          header: '전일대비 %',
          name: 'price_DR',
          align: 'center',
          width:120,
          sortable: true,
          resizable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '5일전대비%',
          name: 'prev_5dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '한달전대비%',
          name: 'prev_20dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '분기전대비%',
          name: 'prev_60dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '반기전대비%',
          name: 'prev_120dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '일년전대비%',
          name: 'prev_240dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: priceRatioColorFmt
        },
        {
          header: '5일가격추이',
          name: 'slope_5dayPrice',
          align: 'right',
          width:120,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: function({ row, column, value }) {return decimal2p(value)},
          filter: {
            type: 'number',
            operator:'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '20일가격추이',
          name: 'slope_20dayPrice',
          align: 'right',
          width:130,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: function({ row, column, value }) {return decimal2p(value)},
          filter: {
            type: 'number',
            operator:'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: 'S-RIM 80%',
          name: 'srim80',
          align: 'right',
          width:100,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: srim_ColumnFmt
        },
        {
          header: '(가격대비 %)',
          name: 'srim80_PR',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          align: 'center',
          //formatter: srimPR_ColumnFmt,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: 'S-RIM 90%',
          name: 'srim90',
          align: 'right',
          width:100,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: srim_ColumnFmt
        },
        {
          header: '(가격대비 %)',
          name: 'srim90_PR',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          align: 'center',
          //formatter: srimPR_ColumnFmt,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: 'S-RIM 100%',
          name: 'srim100',
          align: 'right',
          width:100,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter: srim_ColumnFmt
        },
        {
          header: '(가격대비 %)',
          name: 'srim100_PR',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          align: 'center',
          //formatter: srimPR_ColumnFmt,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '거래량',
          name: 'cur_volumn',
          align: 'right',
          width:100,
          resizable: true,
          sortable: true,
          align: 'center',
          formatter:function({ row, column, value }) {
            var volumn_dr = row.volumn_DR;
            var color = "red";
            if(volumn_dr == 0)
                color = "black";
            else if(volumn_dr < 0)
                color = "blue"
            value = num3Comma(value)
            var rsltFmt = "<p style='text-align:right;color:"+color+"'>"+value+"</p>"
            return rsltFmt;
          }
        },
        {
          header: '(전일대비 %)',
          name: 'volumn_DR',
          align: 'center',
          width:180,
          resizable: true,
          sortable: true,
          align: 'center',
/*          formatter:function({ row, column, value }) {
            var color = "red";
            if(value == 0)
                color = "black";
            else if(value < 0)
                color = "blue"
            value = decimal2p(value)
            var rsltFmt = "<p style='color:"+color+"'>("+value+"%)</p>"
            return rsltFmt;
          },*/
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '전체대비 거래량(%)',
          name: 'volumn_TR',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '매출액(억)',
          name: 'sales',
          align: 'center',
          width:100,
          align: 'right',
          resizable: true,
          sortable: true,
          //formatter:function({ row, column, value }){ return num3Comma(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '영업이익(억)',
          name: 'operating_profit',
          align: 'center',
          width:150,
          align: 'right',
          resizable: true,
          sortable: true,
          //formatter:function({ row, column, value }){ return num3Comma(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '순이익(억)',
          name: 'net_profit',
          align: 'center',
          width:150,
          align: 'right',
          resizable: true,
          sortable: true,
          //formatter:function({ row, column, value }){ return num3Comma(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: 'PER(%)',
          name: 'PER',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: 'ROE(%)',
          name: 'ROE',
          align: 'center',
          width:150,
          resizable: true,
          sortable: true,
          formatter:function({ row, column, value }){ return decimal2p(value); },
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        },
        {
          header: '시가총액(억)',
          name: 'market_cap',
          width:150,
          align: 'right',
          resizable: true,
          sortable: true,
          //formatter:function({ row, column, value }){ return num3Comma(value);},
          filter: {
            type: 'number',
            operator: 'AND',
            showApplyBtn: true,
            showClearBtn: true
          }
        }
      ]
    });

    // 그리드 셀 포커스 변경 이벤트
    stockInfo_gird.on('focusChange', function(e){
        if(infModifyMode) return;   // 데이터 수정 모드에서는 기능 작동하지 않음
        //console.log(e.rowKey);
        // 종목정보 전시
        dispStockInfo(e.rowKey);
        selectedRowKey = e.rowKey;

        // 선택 그리드의 행 배경색을 변경
        if(e.rowKey != e.prevRowKey){
            e.instance.addRowClassName(e.rowKey, 'sell');
            e.instance.removeRowClassName(e.prevRowKey, 'sell');
        }
    });

    // 그리드 더블클릭 이벤트
    stockInfo_gird.on('dblclick', function(e){
        if(infModifyMode) return;   // 데이터 수정 모드에서는 기능 작동하지 않음
        if(e.columnName == "stock_code"){
            // FnGuide 창 열기
            var row = e.instance.getRow(e.rowKey);
            var url = "http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A"+row.stock_code+"&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701";
            window.open(url,"","width=800,height=1000,left=100");
        }

        if(e.columnName == "cur_price"){ // 현재가 컬럼 더블클릭
            // 네이버 주식차트 창 열기
            var row = e.instance.getRow(e.rowKey);
            var url = "https://finance.naver.com/item/fchart.nhn?code="+row.stock_code;
            window.open(url,"","width=800,height=1000,left=100");
        }

        if(e.columnName == "news_count"){ // 관련뉴스 컬럼 더블클릭
            // 네이버 뉴스스 창 열기
           var row = e.instance.getRow(e.rowKey);
            var url = "https://finance.naver.com/item/news.nhn?code="+row.stock_code;
            window.open(url,"","width=800,height=1000,left=100");
        }

    });

    // 그리드 업데이트 시
    stockInfo_gird.on('onGridUpdated', function(e){
        if(infModifyMode) return;   // 데이터 수정 모드에서는 기능 작동하지 않음
        // 그리드 업데이트 시 첫번째 row의 값을 전시
        var rowKey = e.instance.store.data.filteredRawData[0].rowKey;
        // 종목정보 전시
        dispStockInfo(rowKey);
        selectedRowKey = rowKey;
    });
}

$(function(){
    // CSRF 보안처리를 위한 AJAX 통신 Header 설정
    INIT_CSRF_REQUEST_HEADER();

    // 그리드 생성
    initGrid();



    stockInfo_gird.on('beforeRequest', function(ev) {
      // 요청을 보내기 전
    });
    stockInfo_gird.on('response', function(ev) {
      // 성공/실패와 관계 없이 응답을 받았을 경우
      var a = 0;
    });
/*    stockInfo_gird.on('successResponse', function(ev) {
      // 결과가 true인 경우
    });*/
    stockInfo_gird.on('failResponse', function(ev) {
      // 결과가 false인 경우
    });
    stockInfo_gird.on('errorResponse', function(ev) {
      // 오류가 발생한 경우
    });

    // ====================== UI Event ======================
    // 찾기 버튼 이벤트
    $('#btn_search').on('click', function(e){
        e.preventDefault();
        searchStocks();
    });
    // 찾기 취소 버튼 이벤트
    $('#btn_search_cancel').on('click', function(e){
        e.preventDefault();
        $('#search_stock, #search_holdingCompany, #search_customer,'+
    '#search_product, #search_business, #search_subsidiaryCompany').val('')
        searchStocks();
    });

    $('#search_stock, #search_holdingCompany, #search_customer,'+
    '#search_product, #search_business, #search_subsidiaryCompany').on('keydown', function(e){
        if(e.keyCode == 13)
            searchStocks();
    });

    // UI컴포넌트 상태 설정
    function setInputComponentState(modifyMode){
        $('#holdingCompany').prop('readonly', !modifyMode);
        $('#customer').prop('readonly', !modifyMode);
        $('#product').prop('readonly', !modifyMode);
        $('#business').prop('readonly', !modifyMode);
        $('#subsidiaryCompany').prop('readonly', !modifyMode);
        $('#btn_search').attr('disabled', modifyMode);  // 검색 버튼
        $('#btn_search_cancel').attr('disabled', modifyMode); // 검색취소 버튼
//        if(modifyMode)
//            stockInfo_gird.disable();
//        else
//            stockInfo_gird.enable();
    }

    // 종목정보 작성 버튼
    $('#btn_write_stock_info').on('click', function(e){
        e.preventDefault();

        var btnText = infModifyMode?"종목 정보 작성":"작성 완료"
        $('#btn_write_stock_info').text(btnText);

        if(infModifyMode == false){ // 작성시작
            setInputComponentState(true); // 작성컨트롤 활성화
            $('#btn_cancel_stock_info').prop("disabled", false); // 작성취소 버튼 활성화
        }else{ // 작성완료
            setInputComponentState(false); // 작성컨트롤 비활성화
            $('#btn_cancel_stock_info').prop("disabled", true); // 작성취소 버튼 비활성화

            // 작성정보 서버로 전송
            var stock_code = stockInfo_gird.getValue(selectedRowKey, "stock_code");
            var holdingCompany = $('#holdingCompany').val().trim();
            var customer = $('#customer').val().trim();
            var product = $('#product').val().trim();
            var business = $('#business').val().trim();
            var subsidiaryCompany = $('#subsidiaryCompany').val().trim();

            var data = {
                "stock_code":stock_code,
                "holdingCompany":holdingCompany,
                "customer":customer,
                "product":product,
                "business":business,
                "subsidiaryCompany":subsidiaryCompany
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
                stockInfo_gird.setValue(selectedRowKey, "subsidiaryCompany", rslt.subsidiaryCompany); // 종속회사
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

    $(document).ready(function(){
        $("#nav").load("static/html/navigation.html");
    });
});

