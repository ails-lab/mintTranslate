package gr.ntua.ivml.mint.db;


import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collector;

import org.apache.commons.lang3.StringEscapeUtils;
import org.hibernate.SQLQuery;


public class OrganizationDAO extends DAO<Organization, Long> {
	private HashMap<Long, Long> xmlToOrgId = new HashMap<Long,Long>();
	
	public List<Organization> findPrimary() {
		List<Organization> result = Collections.emptyList();
		try {
			result = getSession().createQuery(" from Organization where parentalOrganization is null" ).list();
		} catch( Exception e ) {
			log.error( "Problems: ", e );
		}
		return result;
	}
	
	public	Organization findByName( String name ) {
		Organization result = null;
		try {
			result = (Organization) getSession()
				.createQuery(" from Organization where shortName=:name" )
				.setString("name", name )
				.uniqueResult();
		} catch( Exception e ) {
			log.error( "Problems: ", e );
		}
		return result;
	}

	public	Organization findByFName( String name ) {
		Organization result = null;
		try {
			result = (Organization) getSession()
				.createQuery(" from Organization where englishName=:name" )
				.setString("name", name )
				.uniqueResult();
		} catch( Exception e ) {
			log.error( "Problems: ", e );
		}
		return result;
	}
	
	public List<Organization> findByCountry( String country ) {
		List<Organization> result = null;
		result = getSession()
			.createQuery("from Organization where country=:country " 
						+" order by englishName" )
			.setString("country", country ) 
			.list();
		return result;
	}
	
	public List<Organization> findByProjectIds( List<Integer> projectIds ) {
		StringBuilder sb = new StringBuilder();
		sb.append( "'{");
		sb.append( String.join(",", projectIds.stream().map(i->i.toString())
				.toArray(String[]::new)));

		sb.append( "}'" );
		
		SQLQuery q = getSession().createSQLQuery( "select * from Organization org"
				+ " where project_ids && " + sb 
				+ " order by organization_id" );
		
		q.addEntity("org", Organization.class );

		List<Organization> l = 
				q.list();
		return l;

	}
	
	public List<Organization> findAll() {
		List<Organization> result = null;
		result = getSession()
			.createQuery("from Organization " 
						+" order by englishName" )
			.list();
		return result;
	}
}
