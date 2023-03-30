package gr.ntua.ivml.mint.db;

import java.util.List;

import gr.ntua.ivml.mint.persistent.Project;

public class ProjectDAO extends DAO<Project, Long> {

	public Project findByName(String name) {
		Project result = null;
		try {
			result = (Project) getSession().createQuery(" from Project where name=:name").setString("name", name)
					.uniqueResult();
		} catch (Exception e) {
			log.error("Problems: ", e);
		}
		return result;
	}

	public List<Project> findAll() {
		List<Project> result = null;
		result = getSession().createQuery("from Project").list();
		return result;
	}
}