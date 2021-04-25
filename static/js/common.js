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
    if(!x) return x
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    // Number(changeValue ).toLocaleString()
}

// 소수점 2자리 표현
function decimal2p(x){
    if(!x) return x
    return Math.round(x * 100) / 100
}

// CSRF 보안처리를 위한 AJAX 통신 Header 설정
function INIT_CSRF_REQUEST_HEADER(){
    // 보안처리: CSRF 설정값 얻기
    var csrftoken = $('meta[name=csrf-token]').attr('content')

    // Ajax 통신 전 CSRF 토큰 Request 해더에 설정!!!
    $.ajaxSetup({
        beforeSend: function(xhr, settings) {
            if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type) && !this.crossDomain) {
                xhr.setRequestHeader("X-CSRFToken", csrftoken);
            }
        }
    });
}