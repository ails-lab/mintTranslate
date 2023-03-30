<%--Like Define Items, but for Enrichments... --%>

<%@page import="java.util.HashMap"%>
<%@page import="net.sf.json.*"%>
<%@page import="gr.ntua.ivml.mint.mapping.*"%>
<%@page import="gr.ntua.ivml.mint.db.*"%>
<%@page import="gr.ntua.ivml.mint.persistent.*"%>
<%@ page import="org.apache.log4j.Logger" %>
<%@ page import="gr.ntua.ivml.mint.persistent.User" %>
<%@ page import="gr.ntua.ivml.mint.persistent.Organization" %>

<%@ page import="gr.ntua.ivml.mint.db.DB" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Arrays" %>



<%! public final Logger log = Logger.getLogger(this.getClass());%>

<%
    User user=(User) request.getSession().getAttribute("user");
    if( user != null ) {
        user = DB.getUserDAO().findById(user.getDbID(), false );
    }
%>

<div class="panel-body">
    <div class="block-nav">

        <div class="summary" style="border:none">
            <div id = "all" style="width: 100%; height: 100%">
                <%

                    String enrichmentId = request.getParameter("enrichmentId");
                    String uploadId = request.getParameter("uploadId");
                    if(enrichmentId == null) {
                %>
                <h1>Error: Missing enrichmentId parameter</h1>
                <%
                    }
                    else if (uploadId == null) {
                %>
                <h1>Error: Missing datasetId parameter</h1>
                <%
                    }
                    else {
                %>
                <div id="source_tree" style="position:absolute; width: 40%; padding:10px">
                </div>
                <div id="boxes">

                    <div id="dialog" class="window">

                        <!-- close button is defined as close class -->
                        <a href="#" class="close">Close it</a>

                    </div>
                    <div id="mask"></div>
                </div>
                <div style="position: absolute; right:5px; width: 55%; height:100%; overflow-y: auto; padding: 5px; float:left; margin-left:30px margin-top:5px;">
                <%
                    Enrichment enrichment = DB.getEnrichmentDAO().findById(Long.parseLong(enrichmentId), false);
                    Dataset dataset = DB.getDatasetDAO().findById(Long.parseLong(uploadId), false);
                    List<String> headers = Arrays.asList(enrichment.getHeaders().substring(1, enrichment.getHeaders().length() -1).replaceAll("\\s+","").split(","));
                    log.info(headers);
                    int count = 0;

                    for (String header : headers) {
                        count++;
                %>
                    <div class="row align-middle mb-3">
                        <div class="column-6">
                            <h2 class="mb-0 no-border">Header <%=count%>: "<%=header%>"</h2>
                        </div>
                        <div class="column-6">
                            <div class="xpath_header_<%=count%>_dropdown dropdown">
                                <button id="xpath_header_<%=count%>_dropbtn" class=" dropbtn">Select Header Type</button>
                                <div class="dropdown-content">
                                    <div class="dropdown-choice" id="dropdown_choice_1_xpath_header_<%=count%>" style="cursor: pointer">Insert</div>
                                    <div class="dropdown-choice" id="dropdown_choice_2_xpath_header_<%=count%>" style="cursor: pointer">Match - Exact</div>
                                    <div class="dropdown-choice" id="dropdown_choice_3_xpath_header_<%=count%>" style="cursor: pointer">Match - XPATH</div>
                                </div>
                            </div>
                        </div>
                    </div>


                    <div style="color: #000000; margin-top: -5px; margin-bottom: 5px; margin-left:5px;">
                    </div>
                    <label class="xpath-label" id="xpath_header_<%=count%>_label_dnd" style="display: none">Define XPATH for header</label>
                    <div id="xpath_header_<%=count%>"
                         title="Drag and drop from the list on the left to define the xpath concerning the header."
                         class="schema-tree-drop xpath-header"
                         upload="<%= uploadId %>"
                         data-header="<%=header%>"
                         style="word-wrap: break-word;overflow:hidden;color: #666666; display:none;padding: 3px; height:20px; font-size: 100%; border: 1px solid #CCCCCC;">
                    </div>

                    <br/>
                    <label class="xpath-label" id="xpath_header_<%=count%>_label" class="mt-3" style="display: none; "></label>
                    <textarea class="xpath-input" type="text" id="xpath_header_<%=count%>_input" rows="3" style="display: none;min-width:1168px;"></textarea>

                    <br/>
					<input type="checkbox" id="ordered_chk_<%=count%>" 
					onChange="$(this).nextAll(&quot;span[id^='ordered']&quot;).first().toggle()"
					style="display: none;"
					/> <label for="">Ordered insert</label><span id="ordered_<%=count%>" style="display: none;">  before any of the following elements <input type="text" id="ordered_elements-<%=count %>" columns="20" /></span> 
					<br/>
                <%

                    }
                %>
                    <a id="resetroot" class="navigable focus k-focus">Reset all</a>
                    &nbsp;&nbsp;|&nbsp;&nbsp;
                    <a  id="doneroot" class="navigable focus">Done</a>
                <%
                }
                %>
                </div>
                </div>
            </div>
        </div>
    </div>
</div>

<script type="text/javascript">


    $(document).ready(function() {
        window.scrollTo(0,0);
        const datasetId = <%=uploadId%>;
        const enrichmentId = <%=enrichmentId%>;

        var content = $('#all');
        var panel = content.closest('div[id^="kp"]');
        $K.kaiten('maximize', panel);

        firstload=false;
        uploadId=<%=request.getParameter("uploadId")%>;
        tree = new SchemaTree("source_tree");
        tree.loadFromDataUpload(uploadId);
        tree.refresh();

        function set_path(node, path) {
            if(path.length > 0) {
                node.text(path);
                node.addClass("xpath");
            } else {
                node.text("");
                node.removeClass("xpath");
            }
        }

        function get_path(node) {
            if(node.hasClass("xpath")) return node.text();
            else return "";
        }

        function reset_paths() {
            $('.xpath-header').each(function () {
                set_path($(this), "");
            })

        }

        reset_paths();

        tree.selectNodeCallback = function(data) {
            var xpath = data.xpath;
            var xpathHolderId = data.xpathHolderId;

            var cp = $($("#source_tree").closest('div[id^=kp]'));
            $K.kaiten('removeChildren', cp, false);

            var tokens = xpath.split("/");
            var title = tokens[tokens.length - 1];

            panel = $K.kaiten('load', {
                kConnector:'html.string',
                kTitle: title,
                html: ""
            });

            var details = $("<div>").css("padding", "10px");
            var browser = $("<div>").appendTo(details);

            panel.find(".panel-body").before(details);
            browser.valueBrowser({
                xpathHolderId: xpathHolderId
            });
        };

        tree.dropCallback = function (source,target) {
            set_path($(target), source.xpath);
        };

        var typeChoices = { 1: "INSERT", 2: "EXACT_MATCH", 3: "XPATH_MATCH" }

        // var request = {"datasetId": datasetId, "enrichmentId": enrichmentId, "config": []};
        // var i = 1;
        // $('div[id^=xpath_header_]').each(function () {
        //     request["config"].push({"headerNumber": i})
        //     i++;
        // })

        typeChoice = {}


        $(".dropdown-choice").click(function () {
            let id = this.id;
            let choice = id.charAt(16);
            let header_number = id.slice(31);

            // Store the choice
            typeChoice[header_number] = typeChoices[choice];

            // Change the dropdown button color and text
            let id_button = "xpath_header_"+header_number+"_dropbtn"
            $('#'+id_button).text(this.textContent)
            $('#'+id_button).css("background-color", "#3e8e41")

            // Reset the xpath container and textbox
            set_path($('#xpath_header_'+header_number), "")
            $('#xpath_header_'+header_number+'_input').val("")

            if (choice == "1") {
                // $("#xpath_header_"+header_number+"_xml_fragment_label").css("display", "")
                $("#xpath_header_"+header_number+"_label_dnd").css("display", "")
                $("#xpath_header_"+header_number+"_label_dnd").text("Drag parent element for fragment to insert:")
                $("#xpath_header_"+header_number).css("display", "")
                $("#xpath_header_"+header_number+"_label").css("display", "")
                $("#xpath_header_"+header_number+"_label").text("XML to insert (use {$row/col[..]} or xsl:value-of select=\"$row/\col[..]\" />)")
                $("#xpath_header_"+header_number+"_input").css("display", "")
                const str = 'eg \<dc:description edm:wasGeneratedBy=\"SoftwareAgent\" rdf:resource=\"{$row\/col[2]}\" \/\>'
                $("#xpath_header_"+header_number+"_input").attr( "placeholder", str )

            }
            else if (choice == "2") {
                $("#xpath_header_"+header_number+"_label_dnd").css("display", "")
                $("#xpath_header_"+header_number+"_label_dnd").text("Drag XPATH whose value matches column:")
                $("#xpath_header_"+header_number+"_label").css("display", "none")
                $("#xpath_header_"+header_number).css("display", "")
                $("#xpath_header_"+header_number+"_input").css("display", "none")
            }
            else {
                $("#xpath_header_"+header_number+"_label_dnd").css("display", "")
                $("#xpath_header_"+header_number+"_label_dnd").text("Drag XPATH of field with matching value (goes into $matchValue):")
                $("#xpath_header_"+header_number).css("display", "")
                $("#xpath_header_"+header_number+"_label").css("display", "")
                $("#xpath_header_"+header_number+"_label").text("Enter XPATH expression for matching CSV rows (use $csv/csv/row[...] and $matchValue):")
                $("#xpath_header_"+header_number+"_input").css("display", "")
                $("#xpath_header_"+header_number+"_input").attr( "placeholder", "eg. $csv/csv/row[matches( col[1],  concat( '_',$matchValue,'$'))]")

            }
            console.log(typeChoice)
        });

        $('#resetroot').click(function() {
            reset_paths();
            $('.dropbtn').css("background-color", "#07699f")
            $('.dropbtn').text("Select Header Type")
            $('.xpath-header').css("display", "none")
            $('.xpath-label').css("display", "none")
            $('.xpath-input').css("display", "none")
        });

        $('#doneroot').click(function() {
        	// this panels jquery object
        	var kpan = $(this).closest('div[id^=kp]');
            var flag = false;
            $('.xpath-header').each(function () {
                if ($(this).text()  == "" || $(this).text() == undefined) {
                    flag = true;
                }
            });
            $('.xpath-input').each(function () {
                if ($(this).css('display') != "none" && (($(this).val()  == "" || $(this).val() == undefined))) {
                    flag = true
                }
            })
            if (flag) {
                var $dialog = $('<div></div>')
                .html('Please define all the xpaths to proceed!!')
                .dialog({
                    autoOpen: false,
                    title: 'Missing information',
                    buttons: {
                        Ok: function() {
                            $( this ).dialog( "close" );
                        }
                    }
                });
            	$dialog.dialog('open');
            } else {
                let jsonObj = [];
                let totalHeaderCount = Object.keys(typeChoice).length;
                for (let i = 1; i <= totalHeaderCount; i++) {
                    let tmp = {}
                    tmp["col"] = i;
                    if( typeChoice[i] == "INSERT") {
                    	tmp["type"] = "insert";
                    	tmp["xml"] =  $("#xpath_header_"+i+"_input").val();
                    	tmp["parentPath"] =  $("#xpath_header_"+i).text();
                    	if( $("#ordered_"+i).checked ) {
                    		tmp["beforeElements"] = $("#ordered_"+i).trim().split(/ +/)
                    	}
                    } else if( typeChoice[i] == "EXACT_MATCH") {
                    	tmp["type"] = "exact";
                    	tmp["matchPath"] = $("#xpath_header_"+i).text();
                    } else {
                    	tmp["type"] = "xpathMatch";
                    	tmp["matchPath"] = $("#xpath_header_"+i).text();
                    	tmp["matchExpression"] = $("#xpath_header_"+i+"_input").val();
                    }
                    jsonObj.push(tmp);

                }
				console.log( "Enrichment id: " + enrichmentId );
				console.log( "Upload id: " + uploadId );
				
                $.ajax({
					url: `api/enrichExecute?datasetId=\${uploadId}&enrichmentId=\${enrichmentId}`,
					method: "POST",
				    processData: false,
				    contentType: 'application/json',
					data: JSON.stringify( jsonObj ),
					success: function(o) {
						console.log( JSON.stringify( o ));
        		    	Mint2.modalDialog( Mint2.message( "Your enrichment was successfully queued.", Mint2.OK ), "Enrichment queued", () => {
							var newCurrent = kpan.prev();
					    	
					    	$K.kaiten('slideTo', newCurrent);					 
				    		$K.kaiten('removeChildren', newCurrent, false);
				    		$K.kaiten('reload', newCurrent );
        		    	}

					)},
                    error: function( response ) {
                    	response = response.responseJSON
        		    	Mint2.modalDialog( Mint2.message( response.error, Mint2.ERROR ), "Problem! Maybe try again.", () => {} );
                    }
				});
	
                console.log(jsonObj);
            }
        });

    });
</script>