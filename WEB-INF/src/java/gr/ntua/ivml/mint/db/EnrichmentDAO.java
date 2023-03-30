package gr.ntua.ivml.mint.db;

import gr.ntua.ivml.mint.persistent.Enrichment;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.persistent.Organization;

import java.util.List;

public class EnrichmentDAO extends DAO<Enrichment, Long> {

    public List<Enrichment> findByOrganization(Organization org ) {
        if( org != null )
            return getSession().createQuery("from Enrichment where organization=:org order by organization")
                    .setEntity("org", org)
                    .list();
        else
            return getSession().createQuery("from Enrichment where organization is null order by organization")
                    .list();
    }

}
