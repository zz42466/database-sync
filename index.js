"use strict";
let config     = require('./config.js');
let originDatabase = config.originDatabase;
let targetDatabase = config.targetDatabase;

let mysql      = require('mysql');

let originConnection = mysql.createConnection(originDatabase);
let targetConnection = mysql.createConnection(targetDatabase);

let connects = 0;
let connectCallback = function(err){
	if(err){
		throw err;
		return;
	}
	connects += 1;
	if(connects == 2){
		main();
	}
}


originConnection.connect(connectCallback);
targetConnection.connect(connectCallback);



let originTables = [];

function main(){
	let tableIndex = 0;

	let sql = "SELECT CONCAT(  '`', table_name,  '`,' ) as `sql` FROM information_schema.tables WHERE table_schema ='" + targetDatabase.database + "'";
	targetConnection.query(sql, function(err,data){
		if (err){
			console.log('getTables:' + err);
		}
		let sql = '';
		if(data.length !== 0){
			for(let k in data){
				sql += data[k].sql;
			}
			sql = sql.substr(0,sql.length -1);
			sql = 'DROP TABLE ' + sql;
		}else{
			sql += "SHOW TABLES";
		}
		
		targetConnection.query(sql,function(err){
			if (err){
				console.log('getTables:' + err);
			}

			// return;
			getTables(originConnection,function(tables){
				originTables = tables;
				let copyStruc = function(){
					//复制数据表结构
					if(tableIndex < originTables.length){
						console.log('开始复制'+originTables[tableIndex]+'表结构');
						copyTableStruc(tableIndex,function(){
							console.log(originTables[tableIndex]+'表结构复制完成');
							tableIndex++;
							copyStruc();
						});
					}else{ //复制表数据
						tableIndex = 0;
						copyData();
					}
				}
				let copyData = function(){
					//复制数据表内容
					if(tableIndex < originTables.length){
						console.log('开始复制'+originTables[tableIndex]+'表内容');
						copyTableData(tableIndex,function(){
							console.log(originTables[tableIndex]+'表内容复制完成');
							tableIndex++;
							copyData();
						});
					}else{
						console.log('转移完成');
						process.exit();
					}
				}

				copyStruc();
			});
		});
	});
}



function getTables(connection,callback){
	let sql = 'SHOW TABLES';

	connection.query(sql, function(err, data){
		if (err){
			console.log('getTables:' + err);
		}

		let key = 'Tables_in_' + originDatabase.database;
		let tables = [];
		for(var k in data){
			tables.push(data[k][key].toLowerCase());
		}
		if(typeof callback === 'function'){
			callback(tables);
		}
	});
}

function copyTableStruc(tableIndex, callback){
	let sql = "SHOW CREATE TABLE `" + originTables[tableIndex] + "`";
	originConnection.query(sql, function(err, data){
		if (err){
			console.log('copyTable:' + err);
		}
		let createSql = data[0]['Create Table'];
		targetConnection.query(createSql,function(err){
			if (err) throw err;
			if(typeof callback === 'function'){
				callback();
			}
		});
	});
}

function copyTableData(tableIndex, callback){
	let dataIndex = 0;
	let limits = 100; //每次复制表的数据条数
	let columns = [];
	let table = originTables[tableIndex];
	let sql = "SELECT `COLUMN_NAME` FROM  information_schema.columns WHERE table_schema='"+originDatabase.database+"' AND table_name = '"+table+"'";
	originConnection.query(sql,function(err, data){
		if (err) throw err;

		
		let sql = "SELECT";
		for(let k in data){
			columns.push(data[k]['COLUMN_NAME']);
		}

		let copyOnce = function (){
			sql = "SELECT * FROM `"+table+"` WHERE 1 LIMIT "+dataIndex+" , "+ limits;
			originConnection.query(sql, function(err, data){
				if (err) throw err;

				if(data.length === 0){
					if(typeof callback === 'function'){
						callback();
					}
					return;
				}
				let sql = "INSERT INTO `" + table + "` (";
				for(let k in columns){
					sql += "`"+columns[k] + "`,";
				}
				sql = sql.substr(0,sql.length - 1) + " ) VALUES ";
				for(let k in data){
					sql += "( ";
					for(let m in columns){
						let v = originConnection.escape(data[k][columns[m]])
						sql += "" + v + ",";
					}
					sql = sql.substr(0,sql.length -1) + " ),";
				}
				sql = sql.substr(0,sql.length -1);
				targetConnection.query(sql, function(err, data){
					if (err) throw err;
					dataIndex += limits;
					copyOnce();
				});
			});
		}
		copyOnce();
	});


}