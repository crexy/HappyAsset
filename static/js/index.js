let stockInfo_gird = null; // 종목 정보 그리드


// 종목정보 출력
var printStockValueInfo = function(data){

    var $tbody = $('#stock_value_grid tbody');
    $tbody.empty();
    // 데이터 구조
    // [{stock_name, stock_code, price, srim80, srim90, srim100}, {...},,]
    $.each(data, function(index, item){
        var $tr = $('<tr>',{});
        //var $tr = $('<tr></tr>');

        // 번호
        var $td_no=$("<td>",{text:index+1});
        // 종목명(코드)
        var $td_stockName=$("<td>",{text:item.stock_name+"("+item.stock_code+")"});
        // 현재가(S-RIM 80% 비율)
        var price = num3Comma(item.price);
        var srim80_ratio = item.price / item.srim80 * 100;
        srim80_ratio = Math.round(srim80_ratio*100)/100;
        var $td_price=$("<td>",{text:price+"("+srim80_ratio+"%%)"});
        // S-RIM 80
        var $td_srim80=$("<td>", {text:num3Comma(item.srim80)});
        // S-RIM 90
        var $td_srim90=$("<td>", {text:num3Comma(item.srim90)});
        // S-RIM 100
        var $td_srim100=$("<td>", {text:num3Comma(item.srim100)});

        $tr.append($td_no);
        $tr.append($td_no);
        $tr.append($td_stockName);
        $tr.append($td_price);
        $tr.append($td_srim80);
        $tr.append($td_srim90);
        $tr.append($td_srim100);
        $tbody.append($tr)
    })
}

// 종목S-RIM정보 찾기
function searchStockSRimInfo(){


    var data={
        keyword:$('#search_box').val(),
        //param:Math.random()
    };

    stockInfo_gird.readData(1, data, true);

    //ajax("/stock_srim", 'POST', JSON.stringify(data), printStockValueInfo);
}

// 그리드 생성
function initGrid(){
    let stockInfo_gird = new tui.Grid({
      el: document.getElementById('grid'),
      rowHeaders: ['rowNum'],
      initialRequest: false, // set to true by default
      data:{
        readData: {
            url: '/api/stock_srim',
            method: 'GET',
            contentType: 'application/json',
        }
      }
      scrollX: true,
      scrollY: true,
      columns: [
        {
          header: '종목명',
          name: 'stock_name',
          width:200
        },
        {
          header: '종목코드',
          name: 'stock_code',
          width:100
        },
        {
          header: '현재가',
          name: 'cur_price',
          width:100
        },
        {
          header: '전일가(등락률)',
          name: 'last_price',
          width:100
        },
        {
          header: 'SRIM( 80% | 90% | 100% )',
          name: 'srim',
          width:300
        },
        {
          header: 'SRIM 80% R',
          name: 'srim80R',
          width:100
        },
        {
          header: '거래량',
          name: 'volume',
          width:150
        },
        {
          header: '매출액',
          name: 'sales',
          width:100
        },
        {
          header: '영업이익',
          name: 'oerating_profit',
          width:100
        },
        {
          header: '순이익',
          name: 'net_profit',
          width:100
        },
        {
          header: 'PER',
          name: 'per'
        },
        {
          header: 'ROE',
          name: 'roe'
        },
        {
          header: '시가총액',
          name: 'market_cap',
          width:100
        }
      ]
    });
}

// grid DataSource 형식
var tuigrid_datasource{
  "result": true,
  "data": {
    "contents": [],
    "pagination": {
      "page": 1,
      "totalCount": 100
    }
  }
}

$(function(){
    // CSRF 보안처리를 위한 AJAX 통신 Header 설정
    INIT_CSRF_REQUEST_HEADER();

    // 그리드 생성
    initGrid();

    stockInfo_gird.on('beforeRequest', function(data) {
      // Before sending the request
    }).on('response', function(data) {
      // When a response has been received regardless of success.
    }).on('successResponse', function(data) {
      // When the result is set to true
    }).on('failResponse', function(data) {
      // When the result is set to false
    }).on('errorResponse', function(data) {
      // When an error occurs
    });

    // ====================== UI Event ====================== 
    // 찾기 버튼 이벤트    
    $('#btn_search').on('click', function(e){
        e.preventDefault();
        searchStockSRimInfo();        
    });

    $('#search_box').on('keydown', function(e){
        if(e.keyCode == 13)
            searchStockSRimInfo();
    })
});

