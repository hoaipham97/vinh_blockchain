<!DOCTYPE html>
<html lang="en"><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>
<script src="javascripts/main.js"></script>
<script>jQuery(function($) {
    var locations = {
        'District 1': ['Ward 1', 'Ward 2', 'Ward 3'],
        'District 3': ['Ward 4', 'Ward 5', 'Ward 6'],
        'District 5': ['Ward 7', 'Ward 8', 'Ward 9'],
        'District 10': ['Ward 10', 'Ward 11', 'Ward 12'],
        'Binh Thanh district': ['Ward 14', 'Ward 15', 'Ward 16'],
    }
    
    var $locations = $('#location');
    $('#country').change(function () {
        var country = $(this).val(), lcns = locations[country] || [];
        
        var html = $.map(lcns, function(lcn){
            return '<option value="' + lcn + '">' + lcn + '</option>'
        }).join('');
        $locations.html(html)
    });
});
	

window.onload = function(){
  var id=parseInt(getParameterByName('idhouse'));
  if(!isNaN(id)) {
    HouseRentingContract.houseStructs(parseInt(id),function(err,house){
      document.getElementById('country').value=house[0];
      document.getElementById('price').value=house[6];
      document.getElementById('area').value=house[2];
      document.getElementById('rooms').value=house[3];
      document.getElementById('detail').value=house[4];
    });
   

  }
}

function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, '\\$&');
    var regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)'),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, ' '));
};

function submitHouse() {
  var id=parseInt(getParameterByName('idhouse'));
  var district= document.getElementById('country').value;
  var price=parseInt(document.getElementById('price').value);
  var area=parseInt(document.getElementById('area').value);
  var rooms=parseInt(document.getElementById('rooms').value);
  var detail=document.getElementById('detail').value;
  if(!isNaN(id)) {
    HouseRentingContract.updateHouse(id,district,'ward 1',area,rooms,detail,price,{gas:3000000},function(error,success){
        if(success) alert('Done');
      });
  }
  else {
    HouseRentingContract.newHouse(district,'ward 1',area,rooms,detail,price,{gas:3000000},function(error,success){
        if(success) alert('Done');
      });
  }
  location.reload();
  
};   
  //instance.newHouse('1','ward 1',5,4,'air-conditioner',1000000000000000000,{from:web3.eth.accounts[0],gas:3000000});
  // instance.newHouseContract(0,2018,05,2018,09,'vinhho2508@gmail.com',{from:web3.eth.accounts[1],gas:3000000,value:1000000000000000000});
  //instance.payEachMonth({from:web3.eth.accounts[1],gas:3000000,value:1000000000000000000});
  //instance.deleteHouse(4,{from:web3.eth.accounts[0],gas:3000000});
  //alert(instance.houseStructs(0)[6]);
</script>
<script>
	function resetform() {
		document.getElementById("price").value="";
    document.getElementById("area").value="";
    document.getElementById("rooms").value="";
    document.getElementById("detail").value="";
		document.getElementById("country")[0].selected=true;
	}
</script>

    
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="description" content="">
    <meta name="author" content="">
    <link rel="icon" href="https://getbootstrap.com/favicon.ico">
	<link href="javascripts/jquery-3.3.1.min.js" rel="script">
    <title>Dashboard</title>

    <!-- Bootstrap core CSS -->
    <link href="stylesheets/bootstrap.min.css" rel="stylesheet">

    <!-- Custom styles for this template -->
    <link href="stylesheets/dashboard.css" rel="stylesheet">
  </head>

  <body>
    <nav class="navbar navbar-dark fixed-top bg-dark flex-md-nowrap p-0 shadow">
      <a class="navbar-brand col-sm-3 col-md-2 mr-0" href="file:///C:/Users/vinh-hh-ttv/Desktop/Dashboard%20Template%20for%20Bootstrap.html#">Admin</a>
      <input class="form-control form-control-dark w-100" type="text" placeholder="Search" aria-label="Search">
      <ul class="navbar-nav px-3">
        <li class="nav-item text-nowrap">
          <a class="nav-link" href="file:///C:/Users/vinh-hh-ttv/Desktop/Dashboard%20Template%20for%20Bootstrap.html#">Sign out</a>
        </li>
      </ul>
    </nav>

    <div class="container-fluid">
      <div class="row">
        <nav class="col-md-2 d-none d-md-block bg-light sidebar">
          <div class="sidebar-sticky">
            <ul class="nav flex-column">
              <li class="nav-item">
                <a class="nav-link active" href="./admin">
                  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="feather feather-home"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>
                  Dashboard <span class="sr-only">(current)</span>
                </a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="./form">
                  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="feather feather-file"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>
                  Add new house
                </a>
              </li>           
            </ul>
          </div>
        </nav>

        <main role="main" class="col-md-9 ml-sm-auto col-lg-10 px-4"><div class="chartjs-size-monitor" style="position: absolute; left: 0px; top: 0px; right: 0px; bottom: 0px; overflow: hidden; pointer-events: none; visibility: hidden; z-index: -1;"><div class="chartjs-size-monitor-expand" style="position:absolute;left:0;top:0;right:0;bottom:0;overflow:hidden;pointer-events:none;visibility:hidden;z-index:-1;"><div style="position:absolute;width:1000000px;height:1000000px;left:0;top:0"></div></div><div class="chartjs-size-monitor-shrink" style="position:absolute;left:0;top:0;right:0;bottom:0;overflow:hidden;pointer-events:none;visibility:hidden;z-index:-1;"><div style="position:absolute;width:200%;height:200%;left:0; top:0"></div></div></div>
		   <div class="col-md-6 col-xs-12">
                <div class="x_panel">
                  <div class="x_content">
                    <br />
                    

                      <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12" >Price($)</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
                          <input type="number" min="0" id="price" class="form-control" >
                        </div>
                      </div> 
                       <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12" >Area</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
                          <input type="number" min="0" id="area" class="form-control" >
                        </div>
                      </div> 
                       <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12" >Rooms</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
                          <input type="number" min="0" id="rooms" class="form-control" >
                        </div>
                      </div>                                                                                              
					  <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12">District</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
							<select class="form-control" id="country" name="country" >						
								<option></option>
								<option>District 1</option>
                <option>District 2</option>
								<option>District 3</option>
                <option>District 4</option>
								<option>District 5</option>
                <option>District 6</option>
                <option>District 7</option>
                <option>District 8</option>
                <option>District 9</option>
								<option>District 10</option>
								<option>Binh Thanh district</option>
							</select>
                        </div>
                      </div>  
					  <!-- <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12">Ward</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
							<select class="form-control" id="location" name="location" placeholder="Anycity"></select>
                        </div>
                      </div> -->
					  <div class="form-group">
                        <label class="control-label col-md-3 col-sm-3 col-xs-12">Details</label>
                        <div class="col-md-9 col-sm-9 col-xs-12">
                        <input min="0" id="detail" class="form-control" >
                        </div> 
            </div> 
									<!-- <div class="input-group control-group after-add-more">		
										<input type="text" name="addmore[]" class="form-control" >
										<div class="input-group-btn"> 
											<button class="btn btn-success add-more" type="button"><i class="glyphicon glyphicon-plus"></i> Add</button>
										</div>
									</div>

								</form>
								<!-- Copy Fields-These are the fields which we get through jquery and then add after the above input,-->
								<!-- <div class="copy-fields hide" style="visibility: hidden;">
									<div class="control-group input-group" style="margin-top:10px">
										<input type="text" name="addmore[]" id='detail' class="form-control">
										<div class="input-group-btn"> 
											<button class="btn btn-danger remove" id="as" type="button"><i class="glyphicon glyphicon-remove"></i> Remove</button>
										</div>
									</div>
								</div>
                        </div>
                      </div> -->
					  <div class="form-group">
                      <div class="ln_solid"></div>
                      <div class="form-group">
                        <div class="col-md-9 col-sm-9 col-xs-12 col-md-offset-3">
                          <button type="reset" class="btn btn-primary" onclick="resetform()">Reset</button>
                          <button type="submit" class="btn btn-success" onclick="submitHouse()">Submit</button>
                        </div>
                      </div>
            </div>
            </div>
          </div>
        </main>
      </div>
    </div>
</body></html>
