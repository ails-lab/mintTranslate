
var urid;
var ooid;
var labelsearch;
var importlimit=20; /*number of imports displayed in workspace */

var indent = 1;
var dshtml="";

$.xhrPool = [];
$.xhrPool.abortAll = function() {
    $(this).each(function(idx, jqXHR) {
        jqXHR.abort();
    });
    $.xhrPool.length = 0;
};

$.ajaxSetup({
    beforeSend: function(jqXHR) {
        $.xhrPool.push(jqXHR);
    },
    complete: function(jqXHR) {
        var index = $.xhrPool.indexOf(jqXHR);
        if (index > -1) {
            $.xhrPool.splice(index, 1);
        }
    }
});

function initwait() {
	$.blockUI({ message: '<img src=\"images/rel_interstitial_loading.gif\" /> <br/>Please wait...' });
	
    
}
function endwait(){
	$.unblockUI();
}

/*not finished*/
function ajaxDatasetDelete( dname,uploadId, userId,orgId,kpan) {

	var $dialog = $('<div></div>')
		.html("To successfully delete the dataset <b>"+dname+"</b>, the dataset's  transformations must be deleted first. A deleted transformation will be replaced by its direct precedent if one exists. In case the transformation has been annotated, the original transformation (before the annotations application) will be deleted as well. Are you sure you want to proceed?")
		.dialog({
			autoOpen: false,
			title: 'Delete',
			modal: true,
			buttons: {
				"Continue": function() {
					answer=true;
					$(this).dialog( "close" );
					$.ajax({
						url: "DatasetOptions.action",
						type: "POST",
						data: "action=delete&uploadId=" + uploadId +"&userId=" + userId,
						
						error: function(){
						   	alert("An error occured. Please try again.");
						},
						
						success: function(response) {
						  	if( kpan.find('div.errors').length==0) {
						  		var $dialog = $('<div></div>')
								.html('Datasetet deleted successfully')
								.dialog({
									autoOpen: false,
									title: 'Success',
									buttons: {
										Ok: function() {
											$( this ).dialog( "close" );
										}
									}
								});
						  		$dialog.dialog('open');
	
								// remove children from kpan predecessor
								var newCurrent = kpan.prev();
						    	
						    	$K.kaiten('slideTo', newCurrent);					 
					    		$K.kaiten('removeChildren', newCurrent, false);
					    		$K.kaiten('reload', newCurrent );
						  	}
					    }
					});	  
				},
				Cancel: function() {
					$( this ).dialog( "close" );
				}
		}
	});
	$dialog.dialog('open');
		
}

function ajaxUndoAnnotations(dname, uploadId, userId, orgId, kpan) {
	var answer =false;
	//pass datsetType, to notify for undo on inported datasets as well
	var $dialog = $('<div></div>')
	.html("All annotations applied to <b>"+dname+"</b> will be undone,  " +
			" the original transformation will be recovered.")
	.dialog({
		autoOpen: false,
		title: 'Undo Annotations',
		modal: true,
		buttons: {
			"Continue": function() {
				answer=true;
				$(this).dialog( "close" );
				 $.ajax({
					 url: "DatasetOptions.action",
					 type: "POST",
					 data: "action=undoAnnotations&uploadId=" + uploadId +"&userId=" + userId,
					 error: function() {
					   		alert("An error occured. Please try again.");
					 },
					 success: function(response){
					    //var cp= kpan.find('div.summary > div.info');
					    //cp.html(response);
					    if (kpan.find('div.errors').length > 0) {
					    	alert("An error occured. Please try again.");
					    }
					    else {
					  		var $dialog = $('<div></div>')
							.html('Annotations have been reversed successfully')
							.dialog({
								autoOpen: false,
								title: 'Success',
								buttons: {
									Ok: function() {
										$( this ).dialog( "close" );
									}
								}
							});
					  		 $dialog.dialog('open');
					  		 $K.kaiten('reload',$("#kp2"),{kConnector:'html.page', 
					  			 url:'ImportSummary.action?'+'userId='+userId, kTitle:'My workspace'}, false);
					  		}
						 }  
					});
				  
			},
			Cancel: function() {
				$( this ).dialog( "close" );
			}
		}
	});
	$dialog.dialog('open');
		
}

var hspace=0;
function walk(tree,userId,orgId,term) {
	
	tree.forEach(function(dataset) {
		//console.log('--' + Array(indent).join('--'), node.key);
		var nodename= dataset.name;
		
		console.log("indent:"+indent);
		dshtml='<div id="'+dataset.dataset_id+'" title="'+dataset.name+'"';
		if(dataset.item_hits>0){
		  dshtml+=' onclick="ajaxFetchStatus('+dataset.dataset_id+');var cp=$($(this).closest(\'div[id^=kp]\'));'+
			'$(cp).find(\'div.k-active\').removeClass(\'k-active\');$(this).toggleClass(\'k-active\');'+
			'$K.kaiten(\'removeChildren\',cp, false);'+
			'$K.kaiten(\'load\',{kConnector:\'html.page\', url:\'ItemView.action?uploadId='+dataset.dataset_id+'&userId='+userId+'&organizationId='+ooid+'&query='+term+'\', kTitle:\'Search results\' });"'+
		    'class="items navigable" style="min-height:30px;height:auto;">';}
		else{
			dshtml+=' class="items" style="min-height:30px;height:auto;">';
			
		}
			 dshtml+='<div class="head">';
			 if(indent==1){
			 if(dataset.format.indexOf("OAI")!=-1){
			  dshtml+='<img src="images/oai_symbol.png" class="dsetimage" style="padding-left:'+hspace+'px;">';
		  
			 }
			 else if(dataset.format.indexOf("ZIP-XML")!=-1){
			  dshtml+='<img src="images/zip-icon.png" class="dsetimage" style="padding-left:'+hspace+'px;">';
		  
			 }
			 else if(dataset.format.indexOf("TGZ_XML")!=-1){
				  dshtml+='<img src="images/tgz-icon.png" class="dsetimage" style="padding-left:'+hspace+'px;">';
		     
				 }
			 else if(dataset.format.indexOf("XML")!=-1){
				  dshtml+='<img src="images/xml2.png" class="dsetimage" style="padding-left:'+hspace+'px;">';
		    
				 }
			 else if(dataset.format.indexOf("CSV")!=-1){
			  dshtml+='<img src="images/csv-icon.png" class="dsetimage" style="padding-left:'+hspace+'px;">';
		   
			 }}
			 dshtml+='</div>';
			 if(indent==1){
			 dshtml+='<div class="importLabel">'+nodename+'</div>';}
			 else if(indent>1){
				 dshtml+='<div class="importLabel">'+Array(indent).join('&nbsp;&nbsp;&nbsp;')+'<i class="material-icons my-custom-indent">subdirectory_arrow_right</i>'+nodename+'</div>';
			 }
			 dshtml+='<div class="importInfo">';
			 if(dataset.status!='OK' && dataset.status!='FAILED' && dataset.status!='UNKNOWN'){
			    dshtml+=' <span id="import_stat_'+dataset.dataset_id+'" class="yui-skin-sam"></span>';
			    ajaxFetchStatus(dataset.dataset_id);
		    }
			 else{
				if(!dataset.status_icon){
					if(dataset.status=="OK"){
						dataset.status_icon="images/ok.png";
					}
					else if(dataset.status=="FAILED"){
						dataset.status_icon="images/problem.png";
					}
					else if(dataset.status=="UNKNOWN"){
						dataset.status_icon="";
					}
				}
				dshtml+='<span id="import_stat_'+dataset.dataset_id+'" class="yui-skin-sam">'+
				'<img id="'+dataset.dataset_id+'" src="'+dataset.status_icon+'" style="vertical-align:sub;width:16px;height:16px;" onMouseOver="this.style.cursor=\'pointer\';" title="'+dataset.message+'">'
				+'</span>';
			 }
		 dshtml+="</div><div class='tail'></div><div id='labeldiv' style='margin-top:-10px;'></div></div>";
		 if(indent==1){
		    ajaxFetchProjectlabels(dataset.dataset_id);
		 }
		 $("form[id=importsPanelform]").append(dshtml);
		
		if(dataset.children) {
			indent ++;
			hspace+=10;
			walk(dataset.children,userId,orgId,term);
		}
		if(tree.indexOf(dataset) === tree.length - 1) {
			indent--;
			hspace=hspace-10;
		}
	})
}


function searchForSets(from, limit, term,userId, orgId ) {
	ooid=orgId;
	var loadingCustom = Mint2.loading("Searching for items");
	$("div[id=importsPanel]").empty();
	$("div[id=importsPanel]").append(loadingCustom);
	$.xhrPool.abortAll();
	if(!term){
		ajaxImportsPanel(from, limit, userId,orgId,'',-1);
		return;
	}
	/*var rep={"searchMeta":{"hits":81,"offest":0,"count":0},"datasets":[{"dataset_id":2143,"name":"FashionData.zip","type":"DataUpload","format":"ZIP-XML","status":"OK","item_hits":0,"children":[{"dataset_id":2359,"name":"EDM Transformation","type":"Transformation","format":"ZIP-XML","status":"OK","created":"2014-02-14 12:16:03.917","creator":"Europeana Fashion Admin","item_count":112716,"is_published":false,"item_hits":6,"label_number":0,"message":"Transformation successfull","status_icon":"images/ok.png","schema":"EDM"}]},{"dataset_id":4486,"name":"Catwalk-2014-07-18_data.tgz","type":"DataUpload","format":"TGZ-XML","status":"OK","item_hits":0,"children":[{"dataset_id":5284,"name":"EDM FP Transformation","type":"Transformation","format":"TGZ-XML","status":"OK","created":"2014-11-05 13:35:28.963","creator":"Europeana Fashion Admin","item_count":10364,"is_published":true,"item_hits":38,"label_number":0,"message":"Transformation is stale. Please retransform using updated mappings.","status_icon":"images/redflag.png","schema":"EDM FP"}]},{"dataset_id":1691,"name":"CWP-11-11-2013.tar.gz","type":"DataUpload","format":"TGZ-XML","status":"OK","item_hits":0,"children":[{"dataset_id":5296,"name":"EDM FP Transformation","type":"Transformation","format":"TGZ-XML","status":"OK","created":"2014-11-06 11:34:13.256","creator":"Europeana Fashion Admin","item_count":21077,"is_published":true,"item_hits":17,"label_number":0,"message":"Transformation is stale. Please retransform using updated mappings.","status_icon":"images/redflag.png","schema":"EDM FP"}]},{"dataset_id":2535,"name":"FashioSample.zip","type":"DataUpload","format":"ZIP-XML","status":"OK","created":"2014-02-18 13:56:01.173","creator":"Europeana Fashion Admin","item_count":72,"is_published":false,"item_hits":1,"label_number":0,"message":"Data Upload successfull. Successfully transformed.","status_icon":"images/okblue.png","children":[{"dataset_id":5148,"name":"EDM Transformation_annotated","type":"AnnotatedDataset","format":"ZIP-XML","status":"OK","created":"2014-10-17 22:59:23.242","creator":"Europeana Fashion Admin","item_count":72,"is_published":false,"item_hits":1,"label_number":0,"message":"AnnotatedDataset successfull","status_icon":"images/ok.png","schema":"EDM"},{"dataset_id":2538,"name":"EDM Transformation","type":"Transformation","format":"ZIP-XML","status":"OK","created":"2014-02-18 15:38:17.458","creator":"Europeana Fashion Admin","item_count":72,"is_published":false,"item_hits":1,"label_number":0,"message":"Transformation successfull","status_icon":"images/ok.png","schema":"EDM","children":[  
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               {  
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "dataset_id":2359,
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "name":"EDM Transformation",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "type":"Transformation",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "format":"ZIP-XML",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "status":"OK",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "created":"2014-02-14 12:16:03.917",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "creator":"Europeana Fashion Admin",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "item_count":112716,
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "is_published":false,
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "item_hits":6,
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "label_number":0,
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "message":"Transformation successfull",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "status_icon":"images/ok.png",
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   "schema":"EDM"
	                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                } ]}]},{"dataset_id":6339,"name":"1034_Invalid.zip","type":"DataUpload","format":"ZIP-XML","status":"OK","created":"2015-03-27 11:33:22.75","creator":"Europeana Fashion Admin","item_count":4870,"is_published":false,"item_hits":17,"label_number":0,"message":"Data Upload successfull","status_icon":"images/ok.png","schema":"EDM FP"}]};
  */
  $.ajax({
   	 url: "api/search",
   	 type: "GET",
   	// dataType: "json",
   	 data: "offset=" + from + "&count=" + limit + "&query=label_tg:"+term+"&datasets_only=true&orgid="+orgId,
   	 dataType:'json',
	 crossDomain: true,
	 xhrFields: {
	           withCredentials: true
	      },

     error: function(){
    	
   		console.log("An error occured. Please try again.");
   		},
   	 success: function(response){
   		$("div[id=importsPanel]").empty();
   		 var html="";
   		// response=rep;
   		 if(response.datasets){
   			 
   			 html+='<div class="summary"></div><form id="importsPanelform" name="imports_'+userId+"_"+orgId+'"></form>';
   			 $("div[id=importsPanel]").append(html);
   			 dshtml="";
   			 hspace=0;
   			 indent = 1;
   			 walk(response.datasets,userId,ooid,term);
   	   		
   				//$("form[id=importsPanelform]").append(dshtml);
   		 }
   		 else{html='<div class="summary"></div>   <div class="label">No imports matching your search criteria. Please try again.</div>';
   		      $("div[id=importsPanel]").append(html);}
   	 
   		
   		 
		/*if(!!userId && userId!=-1){
			$("select#filterlabel").val("");
			$("select#filterlabel").trigger('liszt:updated');
		}*/
   		
		
   	 }	
   	  
  });
     
}

 
function ajaxImportsPanel(from, limit, projectId, orgId,labels) {
	var loadingCustom = Mint2.loading("Loading your datasets");
	$("div[id=importsPanel]").empty();
	$("div[id=importsPanel]").append(loadingCustom);

	ooid=orgId;
	labelsearch=labels;

	if(typeof labels == 'undefined'){
		labels="";
	}
	 $.xhrPool.abortAll();
  $.ajax({
   	 url: "ImportsPanel",
   	 type: "POST",
   	 data: {
   		"startImport":from,
   		"maxImports":limit,
   		"projectId":projectId,
   		"orgId": orgId,
   		"labels": labels ? labels.join() : ""
   	 },
     error: function(){
    	console.log("Error in search");
   		},
   	 success: function(response){
   		$("div[id=importsPanel]").empty();
   		 if(!!labels && labels.length>0){
			
			$("select#filteruser").val("-1");
			$("select#filteruser").trigger('liszt:updated');
		}
		
   		$("div[id=importsPanel]").html( response );
		if(from==0){
			
			var numentries=$("div[id=nimports]").text();
			var currentPage = $("div[id=currentPage]").text();
			
			initPagination(numentries, currentPage);
		}
		
		$('div.labelassign').parent().each(function(){
			ajaxFetchProjectlabels($(this).attr("id"));});
		
		ajaxLabelDraw();
		$('div.labelassign').each(function(){
		   $(this).click(function(e){
    		 if($(this).parent().children("div.labelSelector").length){
				
				    $('div').css("pointer-events","auto");
					$(this).parent().children("div.labelSelector").remove();
					$(this).parent().siblings("div").children(".labelSelector").remove();
					return false;
				}
			 else{
				 
				 e.stopPropagation();
				//will attach labels to div with id #labeldiv which is sibling of labelassign button div
				 getLabels(this, $(this).parent().attr("id"));
		   }
		})
		})
   	  }
   	});
     
}




function ajaxDeleteLock(lockid ,kpanel) {
    $.ajax({
    	 url: "LockSummary.action",
		 type: "POST",
		 data: "lockDeletes=" +lockid,
		 error: function(){
		   		alert("An error occured. Please try again.");
		   		},
		 success: function(response){
			  var $dialog = $('<div></div>')
				.html('Lock removed successfully')
				.dialog({
					autoOpen: false,
					title: 'Success',
					buttons: {
						Ok: function() {
							$( this ).dialog( "close" );
						}
					}
				});
		  		$dialog.dialog('open');
			    $K.kaiten('reload', kpanel,response);
			   
			  
			  }
    });
}


function ajaxNotify(uploadId) {
	
	$("<div>").html("Do you want to publish this dataset?").dialog({
	      resizable: false,
	      height:180,
	      buttons: {
	        "Publish": function() {
	      	  $.ajax({
	     		 url: "Publish.action",
	     		 type: "POST",
	     		 data: "uploadId=" + uploadId ,
	     		 error: function(){
	     		   		alert("An error occured. Please try again.");
	     		   		},
	     		 success: function(response){
	     		    
	     		  	 	var $dialog = $('<div></div>')
	     				.html(response.message)
	     				.dialog({
	     					autoOpen: false,
	     					title: 'Success',
	     					buttons: {
	     						Ok: function() {
	     							$( this ).dialog( "close" );
	     						}
	     					}
	     				});
	     		  		$dialog.dialog('open');
	     		  		
	     			 }
	     		});
	          $( this ).dialog( "close" );
	        },
	        Cancel: function() {
	          $( this ).dialog( "close" );
	        }
	      }
	    });	  
	}



function importTransform(uploadId,selectedMapping,orgId){
	
	
	    
	    $.ajax({
	    	 url: "Transform.action",
			 type: "POST",
			 data: "uploadId=" +uploadId+"&selectedMapping="+selectedMapping+ "&organizationId="+orgId,
			 error: function(){
			   		alert("An error occured. Please try again.");
			   		},
			 success: function(response){
			        var data=$.trim(response);
			        
			        var errorval = $(data).filter('div.panel-body').length;
                    if(errorval > 0){
				    	//should render kaiten panel;
				    	 $K.kaiten('load', response);
				    	
				    } else{
				    	var panelcount=$('div[id^="kp"]:last');
				    	var panelid=panelcount.attr('id');
				    	var pnum=parseInt(panelid.substring(2,panelid.length));
				    	var startpanel=$("#kp1");
				    	$K.kaiten('slideTo',startpanel);
				    	if(pnum>3){
				    		var newpanel=$("#kp2");
				    		$K.kaiten('removeChildren',newpanel, false);
				    	   $K.kaiten('reload',newpanel,{kConnector:'html.page', url:'ImportSummary.action?orgId='+orgId+'&userId=-1', kTitle:'My workspace' });

				    	}else{
				    		
				    		 
				    		  $K.kaiten('reload',startpanel,{kConnector:'html.page', url:'ImportSummary.action?orgId='+orgId+'&userId=-1', kTitle:'My workspace' });
				    		}
	
				    	
				    }
				   
				  
				  }
	    });
	

	
}

function ajaxFetchStatus(importId) {
	 $.ajax({
	   	 url: "ImportStatus",
	   	 type: "POST",
	   	 data: "importId="+importId,
	   	 error: function(){
	   		console.log("An error occured. Please try again.");
	   		},
	   	 success: function(response){
	   		 if(response.indexOf('OK')==-1 && response.indexOf('FAILED')==-1 && response.indexOf('UNKNOWN')==-1 ){
		          var fnt="ajaxFetchStatus("+importId+")";
	          	   setTimeout(fnt, 30000);
	          	  $("span[id=import_stat_"+importId+"]").html(response); 
	          	 /*execute every 30 secs*/
	          	 }
	          	 else if(response.indexOf('OK')>-1){
	          		
		          	  $("span[id=import_stat_"+importId+"]").html(response); 
		          	  
	          	 }
	          	 else if(response.indexOf('FAILED')>-1){
	          	  $("span[id=import_stat_"+importId+"]").html(response); 
	          	 }
	          	 else if(response.indexOf('UNKNOWN')>-1){
	          		 /* can no longer find import in db*/
	          		 
	          	 }
	   	  }
	   	});
	
}

function pageselectCallback(page_index, jq){
 	/*find start, end from page_index*/
 	end=(page_index+1)*importlimit;
 	start=end-(importlimit);
 	let projectId = $("#importsPanel").data("projectId");
 	let orgId = $("#importsPanel").data("orgId");
 	let folders = $("#importsPanel").data("folders");
 	ajaxImportsPanel(start, importlimit, projectId, orgId, folders);
    
     
     return false;
 }

 /** 
  * Callback function for the AJAX content loader.
  */
 function initPagination(num_entries,currentPage ) {
     
     // Create pagination element
     $(".imports_pagination").pagination(num_entries, {
    	 num_display_entries:7,
         num_edge_entries: 1,
         callback: pageselectCallback,
         load_first_page:false,
         items_per_page:importlimit,
         current_page:currentPage
     });
  }


