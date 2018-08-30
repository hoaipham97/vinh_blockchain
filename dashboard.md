<!DOCTYPE html>
<!-- saved from url=(0054)https://getbootstrap.com/docs/4.1/examples/dashboard/# -->
<html lang="en">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>
<script src="javascripts/main.js"></script>
<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="description" content="">
    <meta name="author" content="">
    <link rel="icon" href="https://getbootstrap.com/favicon.ico">
	<link href="jquery-3.3.1.min.js" rel="script">
    <title>Dashboard</title>

    <!-- Bootstrap core CSS -->
    <link href="stylesheets/bootstrap.min.css" rel="stylesheet">

    <!-- Custom styles for this template -->
    <link href="stylesheets/dashboard.css" rel="stylesheet">

</head>

<script>
window.onload= function (){
  var tb=document.getElementById(('table'));
 HouseRentingContract.houseCount(function(error,houseCount){
      
  for(let i=0;i<parseInt(houseCount);i++) {
    HouseRentingContract.houseStructs(i,function(err,house){
      if(house[5]==false) {
        var r=document.createElement('tr');
        var t1=document.createElement('input');
        t1.type='checkbox';
        t1.id='chkDetail'+i;
        var t2=document.createElement('td');
        var link=document.createElement('a');
        link.href='./form?idhouse='+i;
        link.innerText=i+1;
        var t3=document.createElement('td');
        var t4=document.createElement('td');
        var t5=document.createElement('td');
        var t6=document.createElement('td');
       // t2.innerText=instance.houseStructs(i)[0];
        t3.innerText=house[0];
        t2.appendChild(link);
        //t4.innerText=instance.houseStructs(i)[3];
        t5.innerText=house[6];
        t6.innerText=house[4];
        r.appendChild(t1);
        r.appendChild(t2);
       // r.appendChild(link);
        r.appendChild(t3);
      //  r.appendChild(t4);
        r.appendChild(t5);
        r.appendChild(t6);
        tb.appendChild(r);
      } 
    }) ;
  }
  });
}

function deleteHouse() {
  HouseRentingContract.houseCount(function(error,houseCount){
        for(var i=0;i<parseInt(houseCount);i++) {
          var check=document.getElementById("chkDetail"+i);
          if(check!==null){
           if(check.checked==true) {
              HouseRentingContract.deleteHouse(i,{from:'0xfcd091c0a2890aeb1ea73c28662f176a1d9d0d0f',gas:3000000},function(error,success){
                  if(success) alert('Done');
            });
            }
          }
        }     
  });
  
}
</script>
<script src="javascripts/socket.io.js"></script>
<script>
  var socket = io.connect('http://localhost:3000');
 socket.on('aa', function (data) {
   console.log(data);
 });
</script>
<script>
  function checkAll() {
      HouseRentingContract.houseCount(function(error,houseCount){
          for(var i=0;i<parseInt(houseCount);i++) {
           var temp=document.getElementById("chkDetail"+i);
           if(temp!=null) {
              temp.checked=document.getElementById("chkAll").checked;
        }
      }
      });
      
  }
  
</script> 
<body>
    <nav class="navbar navbar-dark fixed-top bg-dark flex-md-nowrap p-0 shadow">
      <a class="navbar-brand col-sm-3 col-md-2 mr-0" href="#">Admin</a>
      <input class="form-control form-control-dark w-100" type="text" placeholder="Search" aria-label="Search">
      <ul class="navbar-nav px-3">
        <li class="nav-item text-nowrap">
          <a class="nav-link" href="#">Sign out</a>
        </li>
      </ul>
    </nav>

    <div class="container-fluid">
      <div class="row">
        <nav class="col-md-2 d-none d-md-block bg-light sidebar">
          <div class="sidebar-sticky">
            <ul class="nav flex-column">
              <li class="nav-item">
                <a class="nav-link active" href="#">
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
          <form>
          <div class="table-responsive">
            <table class="table table-striped table-sm" id="table">
              <thead>
                <tr>
                  <th><input type="checkbox" id="chkAll" onclick="checkAll()"></th>
                  <!-- <th>#</th> -->
                  <th>ID</th>
                  <th>Address</th>
                  <th>Price</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                
              </tbody>
            </table>
        
          <button type="submit" onclick="deleteHouse()">Delete</button>
        </form>
          </div>
        </main>
      </div>
    </div>

  
</body>


</html>
