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

$(function(){
    // CSRF 보안처리를 위한 AJAX 통신 Header 설정
    INIT_CSRF_REQUEST_HEADER();
});

