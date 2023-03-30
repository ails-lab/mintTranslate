<%@ taglib prefix="s" uri="/struts-tags" %>

<div id="mustacheTemplate" style="display:none" >
	{{ #options }}
	<div title="{{label}}"
		onclick="{{onclick}}"
		style="{{noButton}}"
		class="items {{navigable}}">
	
		<div class="label">{{label}}</div>
		<div class="info">
			{{#img}}
			<!--  optional image (like loading) -->
			<img data-src="{{src}}"
				style="vertical-align: sub; margin-top: 10px; width: 16px; height: 16px;"
				onMouseOver="this.style.cursor='pointer';" title="{{title}}">
			{{ /img}}
		</div>
		<div class="tail"></div>
	</div>
	{{ /options }}
</div>


<!--  what is the dataset, how to ajax the call -->
<script>

	// this looks like a replace panel with new html style link
	function optionsNewPanel( element, url, label) {
		let thisPanel = $($(element).closest('div[id^=kp]'))
		$K.kaiten('removeChildren',thisPanel, false);
		$K.kaiten('reload',thisPanel,{kConnector:'html.page', url:url, kTitle:label });
	}

	function reloadPanel( elem ) {
		let thisPanel = $($(elem).closest('div[id^=kp]'))
		$K.kaiten('removeChildren',thisPanel, false);
		$K.kaiten('reload', thisPanel );		   		  		
	}

	function apiResponse( url, elem ) {
		 $.ajax({
		   	 url: url,
		   	 type: "GET",
		     error: function( response ){
		    	 console.log( response )
		    	 response = response.responseJSON 	
		    	Mint2.modalDialog( Mint2.message( response.error, Mint2.ERROR ), "" + url, () => reloadPanel( elem )) 
		   		},
		   	 success: function(response) {
		   		 console.log( response )
		    	Mint2.modalDialog( Mint2.message( response.msg, Mint2.OK ), "" + url, () => reloadPanel( elem )) 
		   	  }
		   	});
	}

	function render( data ) {
		// need diffferent onclick handlers for different response types
		for( elem of data ) {
			if( elem.url != null ) {
				elem.navigable = "navigable"
			
				if( elem.response == "json" ) {
					elem.onclick="apiResponse( '" + elem.url + "', this)" 
				} else if( elem.response == "panel" ) {
					elem.onclick="optionsNewPanel( this, '" + elem.url + "'", "'" + elem.label +"')" 
				} else if( elem.response == "popup" ) {
					// html that offers something and goes away
				} else {
					console.log( "response not supported [ " + elem.response + " ] " )
				}
			} else {
				elem.noButton = "cursor:default"
			}
		}
		let elems = Mustache.render( $( "#mustacheTemplate").html(), {"options":data} )
		$( "#mustacheTemplate").replaceWith( elems )
		$(elems).find( "img" ).each( (elem) => elem.src = elem.data( "src" ));
	}
// call the API
  
  
  $.ajax({
	   	 url: "api/datasetOptions/"+ <s:property value="uploadId"/>,
	   	 type: "GET",
	     error: function(){
	   		alert("Error while getting options for Dataset #" + <s:property value="uploadId"/>);
	   		},
	   	 success: function(response) {
	   		 console.log( response )
			 render( response )
	   	  }
	   	});

  
</script>


		