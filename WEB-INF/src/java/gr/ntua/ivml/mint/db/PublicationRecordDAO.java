package gr.ntua.ivml.mint.db;


import java.util.List;
import java.util.Optional;

import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.PublicationRecord;

public class PublicationRecordDAO extends DAO<PublicationRecord, Long> {

	
	public List<PublicationRecord> findByOriginalDataset( Dataset ds ) {
		List<PublicationRecord> result = getSession().createQuery("from PublicationRecord where originalDataset = :ods" )
			.setEntity("ods", ds )
			.list();
		return result;
	}
	
	public List<PublicationRecord> findByOrganization( Organization org ) {
		List<PublicationRecord> result = getSession().createQuery("from PublicationRecord where organization = :org" )
				.setEntity("org", org )
				.list();
			return result;
		
	}

	public List<PublicationRecord> findByAnyDataset(Dataset ds) {
		List<PublicationRecord> result = getSession().createQuery("from PublicationRecord where originalDataset= :ods or publishedDataset = :ods" )
				.setEntity("ods", ds )
				.list();
			return result;
	}
	public Optional<PublicationRecord> getByPublishedDataset(Dataset ds) {
		PublicationRecord result = (PublicationRecord) getSession().createQuery("from PublicationRecord where publishedDataset= :ods" )
				.setEntity("ods", ds )
				.uniqueResult();
		return Optional.ofNullable(result);
	}

	public Optional<PublicationRecord> getByPublishedDatasetTarget(Dataset ds, String target ) {
		PublicationRecord result = (PublicationRecord) getSession().createQuery("from PublicationRecord where publishedDataset= :ods and target = :target" )
				.setEntity("ods", ds )
				.setString("target", target)
				.uniqueResult();
		return Optional.ofNullable(result);
	}
	
	public List<PublicationRecord> pageByTargetDateDesc( String target, int start, int count ) {
		List<PublicationRecord> result = getSession().createQuery(
				"from PublicationRecord where target = :target" 
				+ " order by endDate DESC")
				.setString("target", target)
				.setFirstResult( start )
				.setMaxResults(count)
				.list();
		return result;
	}
}
