CREATE TABLE enrichment
(
    enrichment_id integer NOT NULL,
    name text COLLATE pg_catalog."default",
    headers text COLLATE pg_catalog."default",
    creator_id integer,
    organization_id integer,
    byte_size integer,
    line_count integer,
    project_ids smallint[],
    gzip_csv bytea,
    creation_date timestamp without time zone,
    last_modified timestamp without time zone,
    CONSTRAINT enrichment_pkey PRIMARY KEY (enrichment_id),
    CONSTRAINT enrichment_organization_fk FOREIGN KEY (organization_id)
        REFERENCES carare.organization (organization_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT enrichment_user_fk FOREIGN KEY (creator_id)
        REFERENCES carare.users (users_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE carare.enrichment
    OWNER to carare;