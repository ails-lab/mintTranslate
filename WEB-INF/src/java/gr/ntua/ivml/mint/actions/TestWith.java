package gr.ntua.ivml.mint.actions;

import java.util.Date;
import java.util.Set;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.util.Config;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.sf.json.JSONArray;

@Results({ @Result(name = "json", location = "json.jsp"),
		/*
		 * @Result(name="error", type="json", params={"statusCode", "404",
		 * "root", "${msg}"})
		 */
		@Result(name = "error", type = "httpheader", params = { "error", "404", "errorMessage", "Internal Error" })
		/*
		 * @Result(name= "success", type="httpheader", params={"status","204"}),
		 * 
		 * @Result(name="error", type="httpheader", params={"error", "404",
		 * "errorMessage", "${msg}"})
		 */
})
public class TestWith extends GeneralAction {

	private String token;

	protected final Logger log = Logger.getLogger(getClass());

	private long datasetId;

	private String msg;

	private String collectionName;

	private String collectionDescription;

	private Boolean isPublic = true;

	private JSONObject json = null;

	private String cookie = null;

	public JSONObject getJson() {
		return json;
	}

	public void setJson(JSONObject json) {
		this.json = json;
	}

	final HttpClient hc = new DefaultHttpClient();

	@Action(value = "TestWith")
	public String execute() throws Exception {
		Dataset ds = null;
		try {
			ds = DB.getDatasetDAO().getById(getDatasetId(), false);
		} catch (Exception e) {
			// report some error
			msg = "Couldn't retrieve Dataset [" + getDatasetId() + "]";
			log.error(msg, e);
			// return "error";
			json = new JSONObject();
			json.put("result", msg);
			// json.element( "result",msg);
			return "json";
		}

		if (ds == null) {
			msg = "Couldn't retrieve Dataset [" + getDatasetId() + "]";
			// return "error";
			json = new JSONObject();
			json.put("result", msg);
			return "json";
		}

		if (!user.can("view data", ds.getOrganization())) {
			msg = "User " + user.toString() + " has no right to do that.";
			// return "error";
			json = new JSONObject();
			json.put("result", msg);
			return "json";
		}

		Dataset origin = ds.getOrigin();
		if (origin == null) {
			// that shouldnt happen
			// return "error";
			json = new JSONObject();
			msg = "Something went wrong";
			json.put("result", msg);
			return "json";
		}

		boolean publishable = false;
		Set<String> acceptableSchemaNames = TextParseUtil.commaDelimitedStringToSet(Config.get("with.schemas"));
		String schemaName = null;
		for (Dataset dsi : origin.getDerived()) {
			if (dsi.getSchema() != null) {
				if (acceptableSchemaNames.contains(dsi.getSchema().getName())) {
					publishable = true;
					schemaName = dsi.getSchema().getName();
				}
			}
		}

		if ((schemaName != null) || publishable) {
			Dataset with = origin.getBySchemaName(schemaName);
			try {
				try {
					// log.debug("WITH TOKEN IS : " + token);
					HttpGet loginToWith = new HttpGet(
							Config.get("with.backend") + "/user/loginWithToken?" + "token=" + token);
					HttpResponse response = hc.execute(loginToWith);
					if (response.getStatusLine().getStatusCode() == 200) {
						// hopefully handles the connection ...
						Header[] headers = response.getHeaders("Set-Cookie");
						for (Header h : headers) {
							// log.debug(h.getValue().toString());
							cookie = h.getValue().toString();
						}

						loginToWith.abort();
					} else
						throw new Exception("" + response.getStatusLine());
				} catch (Exception e) {
					log.error("Login to WITH failed", e);
					json = new JSONObject();
					msg = "Login to WITH failed";
					json.put("result", msg);
					throw e;
				}

				final Dataset publishDataset = origin.getBySchemaName(schemaName);
				if (publishDataset == null) {
					throw new Exception("Non With schema export not supported");
				}

				HttpPost createCollection = new HttpPost(Config.get("with.backend") + "/collection");

				net.sf.json.JSONObject jsonBody = new net.sf.json.JSONObject();
				net.sf.json.JSONObject descriptiveData = new net.sf.json.JSONObject();
				net.sf.json.JSONObject label = new net.sf.json.JSONObject();
				JSONArray jsar = new JSONArray();
				jsar.add(collectionName);
				label.put("def", jsar);
				label.put("en", jsar);

				net.sf.json.JSONObject description = new net.sf.json.JSONObject();
				JSONArray jsar2 = new JSONArray();
				jsar2.add(collectionDescription);
				description.put("def", jsar2);
				description.put("en", jsar2);

				descriptiveData.put("label", label);
				descriptiveData.put("description", description);

				net.sf.json.JSONObject administrative = new net.sf.json.JSONObject();
				net.sf.json.JSONObject access = new net.sf.json.JSONObject();

				access.put("isPublic", isPublic);
				administrative.put("access", access);

				jsonBody.put("descriptiveData", descriptiveData);
				jsonBody.put("administrative", administrative);

				createCollection.setEntity(new StringEntity(jsonBody.toString()));
				createCollection.addHeader("Content-type", "text/json");

				HttpResponse response = hc.execute(createCollection);

				String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF8");
				createCollection.releaseConnection();
				createCollection.abort();

				JSONObject obj = (JSONObject) JSONValue.parse(jsonResponse);
				log.debug("Collection created response: " + jsonResponse);

				msg = jsonResponse;
				if (response.getStatusLine().getStatusCode() != 200) {
					// return error message
					json = new JSONObject();
					json.put("result", obj.get("message"));
					return "json";

				}
				final String collectionId = obj.get("dbId").toString();

				PublicationRecord pr = new PublicationRecord();
				pr.setOrganization(origin.getOrganization());
				pr.setStartDate(new Date());
				pr.setStatus(PublicationRecord.PUBLICATION_RUNNING);
				pr.setOriginalDataset(origin);
				pr.setPublishedDataset(with);
				DB.getPublicationRecordDAO().makePersistent(pr);
				DB.commit();
				 gr.ntua.ivml.mint.projects.with.TestWith pub = new gr.ntua.ivml.mint.projects.with.TestWith(origin, pr, collectionId,
					 hc, schemaName,collectionName,isPublic,collectionDescription);
				// we set publication to running, although maybe its not wise to
				// have this as Publication
				Queues.queue(pub, "net");
				msg = jsonResponse;

			} catch (Exception e) {
				// msg = "Login to WITH failed";
				json = new JSONObject();
				msg = "Login to WITH failed";
				json.put("result", msg);
				return "error";

				// return ERROR;

			}

		}

		json = new JSONObject();
		json.put("result", "success");
		return "json";
	}

	public long getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(long datasetId) {
		this.datasetId = datasetId;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public Boolean getIsPublic() {
		return isPublic;
	}

	public void setIsPublic(Boolean isPublic) {
		this.isPublic = isPublic;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getCollectionDescription() {
		return collectionDescription;
	}

	public void setCollectionDescription(String collectionDescription) {
		this.collectionDescription = collectionDescription;
	}

}