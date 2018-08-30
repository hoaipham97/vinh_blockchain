pragma solidity ^0.4.2;
pragma experimental ABIEncoderV2;
contract HousesData {
    
    address public owner; // Mặc định địa chỉ owner == địa chỉ người tạo contract nhà.

    struct houseStruct {
        // arrayList user.
        string district; // quận
        string ward; // 
        uint area; // diện tích
        uint rooms;
        string detail; // Các chủ thích thêm.
        bool isDeleted; // kiểm tra ngôi nhà có bị xóa chưa.
        uint256 price;
        bool status; // trạng thái ngôi nhà
    }
    modifier onlyOwner() {
        if(msg.sender != owner)
    _;
    }
    
    mapping(uint => houseStruct) public houseStructs; // Lưu dữ liệu theo kiểu mapping. index => houseStructs[index].
    uint public houseCount;
    constructor() public { // constructor gán owner = người tạo.
        owner=msg.sender;
    }
    // Tạo ngôi nhà mới.
    function newHouse (string district,
                       string ward, 
                       uint area,
                       uint rooms, 
                       string detail,
                       uint price) 
                       onlyOwner public  returns (uint rowNumber){

        require(msg.sender==owner);
        houseStructs[houseCount].district=district;
        houseStructs[houseCount].ward=ward;
        houseStructs[houseCount].area=area;
        houseStructs[houseCount].rooms=rooms;
        houseStructs[houseCount].detail=detail;
        houseStructs[houseCount].price=price;
        houseStructs[houseCount].isDeleted=false;
        houseStructs[houseCount].status=false;
        houseCount++;
        
    }
    // Update lại thông tin căn nhà.
    function updateHouse(uint id, 
                         string district,
                         string ward, 
                         uint area,
                         uint rooms, 
                         string detail,
                         uint price) 
                         onlyOwner public {
        
        require(msg.sender==owner);
        houseStructs[id].district=district;
        houseStructs[id].ward=ward;
        houseStructs[id].area=area;
        houseStructs[id].rooms=rooms;
        houseStructs[id].detail=detail;
        houseStructs[houseCount].price=price;
        
    }
    // Delete ngôi nhà. Gán tất cả == 0 chứ ko xóa đc.
    function deleteHouse(uint id) onlyOwner public {
        
        require(msg.sender==owner);
        houseStructs[id].isDeleted=true;
        
    }
    function getIndex(uint256 id) onlyOwner public constant returns  (houseStruct v){
        for(uint i = 0; i< houseCount; i++){
            if(i == id) 
                return houseStructs[i];
    }
  }
}
