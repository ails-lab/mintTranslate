package gr.ntua.ivml.mint.view;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.db.XmlSchemaDAO;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.StringUtils;

/**
 * Business logic regarding the View will slowly move here as static methods. 
 * This is a temp place until we have something better.
 * 
 * All methods should contain the parameters needed and not use magic to do their job.
 * 
 * @author arne
 *
 */
public class ViewLogic {
	
	/**
	 * What schemas should given user see when mapping given Dataset.
	 * Conf is fake parameter to show this function accesses Config, until we have something better.
	 * 
	 * @param user
	 * @param ds
	 * @param conf
	 * @param dao
	 * @return
	 */
	public static List<XmlSchema> visibleSchemas( User user, Dataset ds, Config conf, XmlSchemaDAO dao ) {
		List<XmlSchema> allSchemas = dao.findAll();
		
		// superuser, see all schemas
		if( user.hasRight(User.ALL_RIGHTS))
			return allSchemas;

		// no project, see all schemas
		String[] projectNames = ds.getOrigin().getProjectNames();
		if( projectNames.length == 0 )
			return allSchemas;
		
		
		// otherwise only project schemas are visible
		final Set<String> projectSchemas = new HashSet<>();
		for( String projectName: projectNames ) {
		
			String schemaFilter = Config.getWithDefault( projectName+".schemas", "");
			if( !StringUtils.empty(schemaFilter)) {
				projectSchemas.addAll( TextParseUtil.commaDelimitedStringToSet(schemaFilter));
			}
		}
		
		List<XmlSchema> filteredList = allSchemas.stream().filter( schema -> 
			(schema.getJsonTemplate() != null) && (projectSchemas.contains(schema.getName())))
		.collect(Collectors.toList());
		
		return filteredList;
	}
}
