package gr.ntua.ivml.mint.actions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import gr.ntua.ivml.mint.concurrent.Solarizer;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.persistent.XpathStatsValues;
import gr.ntua.ivml.mint.persistent.XpathStatsValues.ValueStat;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@Results({
	@Result(name="error", location="json.jsp"),
	@Result(name="success", location="json.jsp")
})

public class ValueList extends GeneralAction {

	public static final Logger log = Logger.getLogger(ValueList.class ); 
	public JSONObject json;
	public int start, max;
	public String filter;
	public long xpathHolderId;
	
	@Action(value="ValueList")
	public String execute() {
		json = new JSONObject();
		json.element("start", start);
		json.element("max", max);
		json.element("xpathHolderId", xpathHolderId);
		try {
			XpathHolder path = getHolder();
			if( path != null ) {
				json.element("xpath", path.toJSON());
				if(path.getTextNode() != null || path.isAttributeNode()) {
					XpathHolder pathText = (path.isAttributeNode())?path:path.getTextNode();
					json.element("xpathText", pathText.toJSON());
				} else {
					json.element("xpathText", path.toJSON());					
				}

				getValues( path );
//				if( isTotalCount() ) {
//					json.element( "totalCount", path.getDistinctCount() );
//				}
			}
		} catch( Exception e ) {
			json.element( "error", e.getMessage());
			log.error( "No values", e );
		}
		return SUCCESS;
	}
	
	/**
	 * Get the holder and check the read permission of the user.
	 * @return
	 */
	private XpathHolder getHolder() {
		boolean allow = false;

		XpathHolder result = null;
		XpathHolder text = null;

		result = DB.getXpathHolderDAO().getById(xpathHolderId, false);
			
		if( result == null ) {
			json.element( "error", "No such xpath." );
			return null;
		}
		
		
		if( getUser().hasRight(User.SUPER_USER)) allow=true;
		else {
			Organization owner = result.getDataset().getOrganization();
			if( owner == null ) {
				log.warn( "Dataset " + result.getDataset().getDbID() + " belongs to no organization." );
			} else {
				if( getUser().can("view data", owner ) ||
						getUser().sharesProject(result.getDataset().getOrigin())) allow = true;
			}
		}
		
		if( !allow ) {
			json.element( "error", "No access rights" );
			return null;
		}
		
		return result;	
	}
	
	private void getValues( XpathHolder path ) {
		try {
			XpathHolder text = (path.isAttributeNode())?path:path.getTextNode();
			JSONArray theValues = new JSONArray();
			long total = -1;
			
			if(text != null) {
				List<XpathStatsValues.ValueStat> values = null;
				values = text.getValues(getStart(), getMax() );
				List<XpathStatsValues.ValueStat> valuesAll = null;
				valuesAll = text.getValues(getStart(), (int)text.getCount());
				//int distinctCount = findNewDistinctCount(values);
				int distinctCount = calculateDistinctCount(valuesAll);
				text.setDistinctCount(distinctCount);
				for( ValueStat vs: values ) {
					JSONObject valJ = new JSONObject();
					valJ.element("value", vs.value);
					valJ.element("count", vs.count);
					theValues.add(valJ);
				}
				
				total = text.getDistinctCount();
			}
			
			json.element("values", theValues);
			json.element("total", total);
		} catch( Exception e ) {
			log.error( "No values extracted on xpath " + xpathHolderId, e );
		}
	}
	
	public int findNewDistinctCount(List<XpathStatsValues.ValueStat> values) {
		int distinctCount = 0;
		for (XpathStatsValues.ValueStat vs: values) {
			distinctCount += vs.count;
		 }
		return distinctCount;
    }
	
	public int calculateDistinctCount(List<XpathStatsValues.ValueStat> values) {
		int distinctCount = 0;
		Set<String> valueNames = new HashSet<String>();
		for (XpathStatsValues.ValueStat vs: values) {
			if(!valueNames.contains(vs.value)) {
				distinctCount += 1;
				valueNames.add(vs.value);
			}
		}
		return distinctCount;
	}
	
	public int calculateAnnotatedDatasetDistinctCount(String id) {
		int distinctCount = 0;
		SolrQuery sq = new SolrQuery();
		sq.setStart(1);
		sq.setQuery("dataset_id:"+id);
		sq.setFacet(true);
		sq.addFacetField("dataset_id");
		sq.setParam( "facet.limit", "-1" );
		QueryResponse resp;
		FacetField ff;
		try {
			resp = Solarizer.getSolrClient().query(sq);
			ff = resp.getFacetFields().get(0);
			List<Count> counts = ff.getValues();
		} catch (Exception e) {
			log.debug("Cannot receive information from solr server!", e);
		}
		
		return -1;
	}
	
	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public long getXpathHolderId() {
		return xpathHolderId;
	}

	public void setXpathHolderId(long xpathHolderId) {
		this.xpathHolderId = xpathHolderId;
	}

	public void setJson(JSONObject json) {
		this.json = json;
	}

	public JSONObject getJson() {
		return json;
	}
}
