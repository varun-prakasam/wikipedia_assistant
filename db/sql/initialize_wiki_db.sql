CREATE DATABASE IF NOT EXISTS wikipedia_etl;
GO


DROP TABLE IF EXISTS wikipedia_etl.production_table_names;
GO

CREATE TABLE wikipedia_etl.production_table_names(
    table_name VARCHAR(255) NOT NULL,
    filename_format VARCHAR(255) NOT NULL
);
GO

INSERT INTO wikipedia_etl.production_table_names VALUES
('categorylinks',       'simplewiki-{YYYYMMDD}-categorylinks.sql.gz'),
('category',            'simplewiki-{YYYYMMDD}-category.sql.gz'),
('page',                'simplewiki-{YYYYMMDD}-page.sql.gz'),
('pagelinks',           'simplewiki-{YYYYMMDD}-pagelinks.sql.gz'),
('page_props',          'simplewiki-{YYYYMMDD}-page_props.sql.gz'),
('redirect',            'simplewiki-{YYYYMMDD}-redirect.sql.gz'),
('sites',               'simplewiki-{YYYYMMDD}-sites.sql.gz'),
('site_stats',          'simplewiki-{YYYYMMDD}-site_stats.sql.gz'),
('templatelinks', 'simplewiki-{YYYYMMDD}-templatelinks.sql.gz'),
('user_groups',         'simplewiki-{YYYYMMDD}-user_groups.sql.gz'),
('pagelinkpositions',   'simplewiki-{YYYYMMDD}-pages-meta-current.xml.bz2');
GO


DROP TABLE IF EXISTS wikipedia_etl.etl_job_log;
GO

CREATE TABLE wikipedia_etl.etl_job_log(
    job_id INT UNSIGNED NOT NULL,
    wiki_dump_date DATE NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    downloaded_file_path VARCHAR(511),
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL
);
GO


CREATE DATABASE IF NOT EXISTS wikipedia;
GO
