

// Ajax 호출
// var ajax = function(url, type, dataType, contentType, data, callbackFun){
//     $.ajax({
//         url:url,
//         type:type,
//         dataType:dataType,
//         contentType:contentType,
//         data:data,
//         catche:false,
//         success:callbackFun,
//         error:function(request, status, error){
//             alert(error);
//             console.log(request);
//             console.log(status);
//             console.log(error);
//         }
//     })
// }

var ajax = function(url, type, data, callbackFun){
    $.ajax({
        url:url,
        type:type,
        data:data,
        contentType: "application/json",
        dataType: 'json',
        catche:false,
        success:callbackFun,
        error:function(request, status, error){
            alert(error);
            console.log(request);
            console.log(status);
            console.log(error);
        }
    })
}

// 숫자 3자리 콤마
function num3Comma(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// 종목정보 출력
var printStockValueInfo = function(data){
    alert('Ok');
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

        var $tbody = $('#stock_value_grid tbody');
        
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
    var csrftoken = $('meta[name=csrf-token]').attr('content')

    // Ajax 통신 전 CSRF 토큰 Request 해더에 설정!!!
    $.ajaxSetup({
        beforeSend: function(xhr, settings) {
            if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type) && !this.crossDomain) {
                xhr.setRequestHeader("X-CSRFToken", csrftoken);
            }
        }
    });

    $('#btn_search').on('click', function(e){
        e.preventDefault();

        var data={
            keyword:$('#search_box').val(),
            //param:Math.random()
        };        

        //ajax("/stock_srim", 'GET', 'json', 'application/json', JSON.stringify(data), printStockValueInfo);
        ajax("/stock_srim", 'POST', JSON.stringify(data), printStockValueInfo);
    })
});

