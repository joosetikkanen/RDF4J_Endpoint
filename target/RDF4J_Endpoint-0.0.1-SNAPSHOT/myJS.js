"use strict";


function doQuery(){
	let server = document.getElementById("serverLocation").value;
	let repoID = document.getElementById("repoID").value;
	let query = document.getElementById("queryText").value;
	
	let reqBody = JSON.stringify({
		"server": server,
		"id": repoID,
		"query": query
	});
	
	doAjax('ClientServlet',reqBody,'query_back','post',0);
	
}

function query_back(result){
	
	let resultTable = document.getElementById("result");
	resultTable.textContent = '';
	let json = JSON.parse(result);
	
	let tr = document.createElement("tr");
	resultTable.appendChild(tr);
	
	let lists = [];
	
	for (const key of Object.keys(json)){
		let th = document.createElement("th");
		th.textContent = key;
		tr.appendChild(th);
		lists.push(json[key])
	}
	
	for (let i = 0; i < lists[0].length; i++){
		
		let tr = document.createElement("tr");
		resultTable.appendChild(tr);
		
		for (let j = 0; j < lists.length; j++){
			let td = document.createElement("td");
			td.textContent = lists[j][i];
			tr.appendChild(td);
		}
		
	}
	
}

function handleError(message){
	let resultTable = document.getElementById("result");
	resultTable.textContent = '';
	let tr1 = document.createElement("tr");
	resultTable.appendChild(tr1);
	let th = document.createElement("th");
	th.textContent = "Error";
	tr1.appendChild(th);
	
	let tr2 = document.createElement("tr");
	resultTable.appendChild(tr2);
	let td = document.createElement("td");
	td.textContent = message;
	tr2.appendChild(td);
}