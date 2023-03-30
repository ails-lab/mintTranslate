package gr.ntua.ivml.mint.db;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.SQLQuery;

import gr.ntua.ivml.mint.concurrent.Solarizer;
import gr.ntua.ivml.mint.concurrent.TransformHotdir;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;

public class DatasetDAO extends DAO<Dataset, Long> {
	public static final Logger log = Logger.getLogger(DatasetDAO.class);
		
	public List<Dataset> findByOrganizationUser( Organization o, User u ) {
		List<Dataset> l = getSession().createQuery( "from Dataset where organization = :org and creator = :user order by last_modified DESC" )
			.setEntity("org", o)
			.setEntity("user", u)
			.list();
		return l;
	}
	
	public List<Dataset> findByOrganization( Organization o) {
		List<Dataset> l = getSession().createQuery( "from Dataset where organization = :org order by last_modified DESC" )
			.setEntity("org", o)
			.list();
		return l;
	}
	
	public Dataset findByName( String name ) {
		Dataset ds = (Dataset) getSession().createQuery( "from Dataset where name=:name")
		 .setString("name", name)
		 .uniqueResult();
		return ds;
	}
	
	public List<Dataset> findPublishedByOrganization( Organization org ) {
		List<Dataset> l = getSession().createQuery( "originalDataset from PublicationRecord where organization = :org and status=:stat" )
		.setEntity("org", org)
		.setString( "stat", Dataset.PUBLICATION_OK)
		.list();
		return l;
	}
	
	
	public List<Dataset> findNonDerivedByOrganization( Organization org ) {
		List<Dataset> l = getSession().createQuery( "from Dataset ds where ds.class <> 'Transformation' " 
				+ "		and ds.class <> 'AnnotatedDataset' and organization = :org order by last_modified DESC" )
				.setEntity("org", org)
				.list();
			return l;		
	}
	
	public List<Dataset> findNonDerivedByOrganizationUser( Organization o, User u ) {
		List<Dataset> l = getSession().createQuery( "from Dataset ds where ds.class <> 'Transformation' "
				+ "  and ds.class <> 'AnnotatedDataset' and organization = :org and  creator = :user order by last_modified DESC" )
			.setEntity("org", o)
			.setEntity("user", u)
			.list();
		return l;
	}
	
	public List<Dataset> findNonDerivedByOrganizationFolders( Organization o, String... folders ) {
		StringBuilder sb = new StringBuilder();
		ArrayList<String> likeParams = new ArrayList<String>();
		int i =0;
		for( String folder: folders ) {
			String jsonFolder = "%" + folder + "%";
			sb.append( " and jsonFolders like :param" + i );
			likeParams.add( jsonFolder);
			i++;
		}
		Query q = getSession().createQuery( "from Dataset ds where ds.class <> 'Transformation' "
				+ "		and ds.class <> 'AnnotatedDataset' and organization = :org " + sb.toString() 
				+ " order by last_modified DESC ")
				.setEntity("org", o);
		for( int j=0; j<i; j++ ) {
			q.setString("param"+j, likeParams.get(j));
		}
		List<Dataset> l = 
				q.list();
		return l;
	}

	// can be used with org=null to get all project datasets out
	public List<Dataset> findNonDerivedByProjectFolders( Organization org, Long projectId, String... folders ) {
		StringBuilder sb = new StringBuilder();
		ArrayList<String> likeParams = new ArrayList<String>();
		int i =0;
		for( String folder: folders ) {
			String jsonFolder = "%" + folder + "%";
			sb.append( " and json_folders like :param" + i );
			likeParams.add( jsonFolder);
			i++;
		}
		SQLQuery q = getSession().createSQLQuery( "select * from Dataset ds left join data_upload du on ds.dataset_id=du.dataset_id " 
				+  " where ds.subtype in( 'Dataset', 'DataUpload' ) "
				+ ((org!=null) ? " and organization_id = :org" : "")
				+ "	 and ds.project_ids @> ARRAY["+projectId+"]\\:\\:smallint[] " + sb.toString()
				+ " order by organization_id, last_modified desc" );
		for( int j=0; j<i; j++ ) {
			q.setString("param"+j, likeParams.get(j));
		}
		q.setLong("org", org.getDbID());
		q.addEntity("ds", Dataset.class );
		
		List<Dataset> l = 
				q.list();
		return l;
	}

	/**
	 * This method overrides method from  parent class
	 * 
	 * @param entity
	 * @return
	 */
	@Override
	public boolean makeTransient(Dataset ds) {
		String dsId = ds.getDbID().toString();
		int id = ds.getDbID().intValue();

		if(super.makeTransient(ds)) {
			if( Solarizer.isEnabled()) {
				try {
				Solarizer.delete("dataset_id:"+dsId);
				Solarizer.commit();
				} catch( Exception e ) {
					log.error( "Solr delete dataset #"+dsId+ " failed!");
				}
			}
			// support transformation hotdir
			TransformHotdir hotdir = DB.getHotdir();
			if( hotdir != null ) hotdir.removeDs( id, null );

			return true;
		}
		else
			return false;
	}

	
	public void cleanup() {
		DB.getDataUploadDAO().cleanup();
		DB.getAnnotatedDatasetDAO().cleanup();
		DB.getTransformationDAO().cleanup();
	}
	

}
