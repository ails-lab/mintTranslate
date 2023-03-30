package gr.ntua.ivml.mint.actions;

import java.util.List;
import java.util.Set;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.InterceptorRef;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.util.Config;
import nu.xom.Document;

/**
 * pass in the EUscreen id and get metadata html
 * @author stabenau
 *
 */

@Results({
	  @Result(name="success", location="xom_output.jsp")
})

public class GoogleStandin extends GeneralAction {
	public String id;
	private Item item;
	
	Set<String> schemas= TextParseUtil.commaDelimitedStringToSet(Config.get("euscreen.portal.schema"));

	@Action(value="EuscreenItem",interceptorRefs=@InterceptorRef("defaultStack"))
	public String execute() {
		List<Object[]> items = DB.getItemDAO().getPublishedItems(id);
		for( Object[] row: items ) {
			Item i = (Item) row[0];
			String schema = (String) row[1];
			
			if( schemas.contains(schema)) this.item = i;
		}
		if( item != null )
			return SUCCESS;
		else
			return NONE;
	}
	
	public String getXml() {
		Document d = item.getDocument();
		String goodGoogleTitle =
			item.getValue("//*[local-name='TitleSetInEnglish']/*[local-name()='title'");
		goodGoogleTitle += "\n" + item.getValue("//*[local-name()='identifier']" );
		d.getRootElement().insertChild(goodGoogleTitle, 0 );
		String xml = d.toXML();
		xml = xml.replaceAll("^(<\\?.*)", "" );
		String head = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
		String xsl = "<?xml-stylesheet type=\"text/xsl\" href=\"xsl/EUscreenToGoogle.xsl\"?>\n";
		return head+xsl+xml;
	}
	
//
//  Getter setter
//
	public String getId() {
		return id;
	}
	public void setItemId(String id) {
		this.id = id;
	}
}
