<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Chi tiết nhà</title>
<!-- Style CSS -->
<link rel="stylesheet" href="stylesheets/style.css">
</head>
</head>
<body>
<div id="preloader">
    <div class="south-load"></div>
</div>
<!-- ##### Header Area Start ##### -->
<header class="header-area">
    <!-- Top Header Area -->
    <div class="top-header-area">
        <div class="h-100 d-md-flex justify-content-between align-items-center">
            <div class="email-address">
                <a href="mailto:contact@southtemplate.com">contact@southtemplate.com</a>
            </div>
            <div class="phone-number d-flex">
                <div class="icon">
                    <img src="images/icons/phone-call.png" alt="">
                </div>
                <div class="number">
                    <a href="tel:+45 677 8993000 223">+45 677 8993000 223</a>
                </div>
            </div>
        </div>
    </div>

    <!-- Main Header Area -->
    <div class="main-header-area" id="stickyHeader">
        <div class="classy-nav-container breakpoint-off">
            <!-- Classy Menu -->
            <nav class="classy-navbar justify-content-between" id="southNav">

                <!-- Logo -->
                <a class="nav-brand" href="/"><img src="images/core-img/logo.png" alt=""></a>

                <!-- Navbar Toggler -->
                <div class="classy-navbar-toggler">
                    <span class="navbarToggler"><span></span><span></span><span></span></span>
                </div>

                <!-- Menu -->
                <div class="classy-menu">

                    <!-- close btn -->
                    <div class="classycloseIcon">
                        <div class="cross-wrap"><span class="top"></span><span class="bottom"></span></div>
                    </div>

                    <!-- Nav Start -->
                    <div class="classynav">
                        <ul>
                            <li><a href="/">Home</a></li>
                            <li><a href="#">About Us</a></li>
                            <li><a href="#">Mega Menu</a>
                                <div class="megamenu">
                                    <ul class="single-mega cn-col-4">
                                        <li class="title">Headline 1</li>
                                        <li><a href="#">Mega Menu Item 1</a></li>
                                        <li><a href="#">Mega Menu Item 2</a></li>
                                        <li><a href="#">Mega Menu Item 3</a></li>
                                        <li><a href="#">Mega Menu Item 4</a></li>
                                        <li><a href="#">Mega Menu Item 5</a></li>
                                    </ul>
                                </div>
                            </li>
                            <li><a href="#">Contact</a></li>
                        </ul>

                    </div>
                    <!-- Nav End -->
                </div>
            </nav>
        </div>
    </div>
</header>
<!-- ##### Header Area End ##### -->

<!-- ##### Advance Search Area Start ##### -->
<div class="south-search-area" style="padding-top: 180px">
    <div class="container">
        <div class="row">
            <div class="col-12">
                <div class="advanced-search-form">
                    <!-- Search Title -->
                    <div class="search-title">
                        <p>Search for your home</p>
                    </div>
                    <!-- Search Form -->
                    <form action="Welcome" method="post" id="advanceSearch">
                        <div class="row">

                            <div class="col-12 col-md-4 col-lg-3">
                                <div class="form-group">
                                    <select class="form-control" id="cities">
                                        <option value="-1">-Quận-</option>
                                        <option>Quận 1</option>
                                        <option>Quận 2</option>
                                        <option>Quận 3</option>
                                        <option>Quận 4</option>
                                        <option>Quận 5</option>
                                        <option>Quận 6</option>
                                        <option>Quận 7</option>
                                        <option>Quận 8</option>
                                        <option>Quận 9</option>
                                        <option>Quận 10</option>
                                    </select>
                                </div>
                            </div>

                            <div class="col-12 col-md-8 col-lg-12 col-xl-5 d-flex">
                                <!-- Space Range -->
                                <div class="slider-range">
                                    <div data-min="12" data-max="30" data-unit=" m2" class="slider-range-price ui-slider ui-slider-horizontal ui-widget ui-widget-content ui-corner-all" data-value-min="12" data-value-max="30">
                                        <div class="ui-slider-range ui-widget-header ui-corner-all"></div>
                                        <span class="ui-slider-handle ui-state-default ui-corner-all" tabindex="0"></span>
                                        <span class="ui-slider-handle ui-state-default ui-corner-all" tabindex="0"></span>
                                    </div>
                                    <div id="square-range" class="range">12 m2 - 30 m2</div>
                                </div>

                                <!-- Price Range -->
                                <div class="slider-range" id="dady">
                                    <div data-min="1" data-max="5" data-unit=" triệu" class="slider-range-price ui-slider ui-slider-horizontal ui-widget ui-widget-content ui-corner-all" data-value-min="1" data-value-max="5">
                                        <div class="ui-slider-range ui-widget-header ui-corner-all"></div>
                                        <span class="ui-slider-handle ui-state-default ui-corner-all" tabindex="0"></span>
                                        <span class="ui-slider-handle ui-state-default ui-corner-all" tabindex="0"></span>
                                    </div>
                                    <div id="price-range" class="range">1 triệu - 5 triệu</div>
                                </div>
                            </div>

                            <div class="col-12 d-flex justify-content-between align-items-end">
                                <!-- Submit -->
                                <div class="form-group mb-0">
                                    <button type="button" class="btn south-btn" onclick="search_click()">Search</button>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>
<!-- ##### Advance Search Area End ##### -->

<!-- ##### Detail Area Begin -->
<div style="padding-top: 20px">
	<!-- Image -->
	<div align="center">
		<img alt="" src="images/bg-img/feature2.jpg" style="width: 500px; height: 362px;">
	</div>
	<!-- Info -->
	<div align="center">
		<p id="price">Tiền thuê:&emsp;</p>
		<p id="area">Kích thước:&emsp;</p>
		<p id="detail"> Mô tả căn nhà:&emsp;</p>
	</div>
	<!-- Button -->
	<div align="center">
		<button type="button" class="btn south-btn" data-toggle="modal" data-target="#modalThuePhong">Thuê phòng</button>
		<div>
			<!-- Modal -->
			<div class="modal fade" id="modalThuePhong" role="dialog" style="padding-top: 90px;" align="left">
				<div class="modal-dialog" style="max-width: 80%">
					<!-- Modal content -->
					<div class="modal-content">
						<div class="modal-header">
							<h4 style="color: #7d7d7d;">CHẤP NHẬN ĐIỀU KHOẢN</h4>
							<button type="button" class="close" data-dismiss="modal">&times;</button>
						</div>
						<div class="modal-body">
							<p id="modalPrice">Tiền thuê hằng tháng: </p>
							<p id="modalArea">Diện tích: </p>
              <div style="color: #7d7d7d; font-size: 14px; line-height: 2; font-weight: 600;">
                <label>Thời gian muốn thuê: </label><br/>
                <label>Từ tháng&emsp;</label><input type="text" id="monthStart" style="width:30px" maxlength="2"><label>&emsp;Năm&emsp;</label><input type="text" id="yearStart" style="width:60px" maxlength="4">
                <br/>
                <label>Đến tháng&emsp;</label><input type="text" id="monthEnd" style="width:30px" maxlength="2"><label>&emsp;Năm&emsp;</label><input type="text" id="yearEnd" style="width:60px" maxlength="4">
                <br/>
                <label>Email&emsp;</label><input type="text" id="email" style="width:200px" >
              </div>
							<p style="text-decoration: underline;">*Lưu ý</p>
      				<p>Thời hạn hợp đồng là 01 năm kể từ lúc thuê. Người thuê phải gia hạn hợp đồng khi hết hạn, nếu không bên cho thuê sẽ chấm dứt hợp đồng.</p>
      				<p>Tiền thuê hằng tháng sẽ được nhắc nhở qua email của người thuê trước 05 ngày, và được thu vào ngày đầu tiên (ngày 01) của tháng đó. Người thuê không thanh toán tiền thuê trong vòng 07 ngày kể từ ngày đầu tháng thì sẽ chấm dứt hợp đồng và không nhận lại được tiền cọc đã đặt trước.</p>
      				<p>Người thuê có thể kết thúc hợp dồng khi hợp đồng hết hạn và nhận lại tiền cọc đặt trước. Trường hợp chấm dứt hợp dồng sớm thì chỉ nhận lại 20% số tiền đặt cọc.</p>
      				<p>Xác nhận thuê phòng bằng cách nhấn vào nút Xác nhận bên dưới để tiến hành thanh toán.</p>
       			</div>
     				<div class="modal-footer">
     					<button type="button" class="btn btn-primary" onclick="confirmHouseRenting()">Xác nhận</button>
		          <button type="button" class="btn btn-default" data-dismiss="modal">Đóng</button>
		        </div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
<!-- ##### Detail Area End -->

<!-- jQuery (Necessary for All JavaScript Plugins) -->
<script src="https://code.jquery.com/jquery-2.2.4.min.js"></script>
<!-- Popper js -->
<script src="javascripts/popper.min.js"></script>
<!-- Bootstrap js -->
<script src="javascripts/bootstrap.min.js"></script>
<!-- Plugins js -->
<script src="javascripts/plugins.js"></script>
<script src="javascripts/classy-nav.min.js"></script>
<script src="javascripts/jquery-ui.min.js"></script>
<!-- Active js -->
<script src="javascripts/active.js"></script>
<script src="javascripts/main.js"></script>
</body>
<script type="text/javascript">
    var temp;
HouseRentingContract.houseStructs(<%= houseId%>,function(error, house) {
  if (!error) {
    temp=parseInt(house[6]);
    $('#modalPrice')[0].innerText += web3.fromWei(house[6],'ether')+ ' ETH';
    $('#price')[0].innerText += web3.fromWei(house[6],'ether')+ ' ETH';
    $('#modalArea')[0].innerText += parseInt(house[2]) + ' m2';
    $('#area')[0].innerText += parseInt(house[2]) + ' m2';
    $('#detail')[0].innerText += house[4];
  }
});

function confirmHouseRenting() {
  //check for valid date
  let monthStart = $('#monthStart').val(), monthEnd = $('#monthEnd').val(), yearStart= $('#yearStart').val(), yearEnd = $('#yearEnd').val();
  HouseRentingContract.newHouseContract(<%=houseId%>,yearStart,monthStart,yearEnd,monthEnd,document.getElementById('email').value,{value:temp},function(error,success){
    if(error) {
        alert(':(');
    }
  });
}
</script>
</html>
