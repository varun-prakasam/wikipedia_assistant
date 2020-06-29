USE wikipedia;
GO

DROP TABLE IF EXISTS most_outdated_page;
GO

CREATE TABLE most_outdated_page(
    category_id int(10) unsigned NOT NULL,
    category_title VARBINARY(255) NOT NULL,
    most_outdated_page_id INT(8) UNSIGNED NOT NULL,
    most_outdated_page_namespace INT(11) NOT NULL,
    most_outdated_page_title VARBINARY(255) NOT NULL,
    most_outdated_page_modification_time TIMESTAMP NOT NULL,
    most_recently_modified_link_page_id INT(8) UNSIGNED NOT NULL,
    most_recently_modified_link_page_namespace INT(11) NOT NULL,
    most_recently_modified_link_page_title VARBINARY(255) NOT NULL,
    most_recently_modified_link_update_time TIMESTAMP NOT NULL,
    outdatedness_in_seconds BIGINT,
    num_pages_in_category INT NOT NULL
);
GO

INSERT INTO most_outdated_page (

SELECT t2.cat_id AS category_id,
       CONVERT(t1.cl_to, VARCHAR(255)) AS category_title,
       outdated.pl_from AS most_outdated_page_id,
       outdated.pl_from_namespace AS most_outdated_page_namespace,
       outdated.pl_from_title AS most_outdated_page_title,
       outdated.last_modified AS most_outdated_page_modification_time,
       outdated.link_page_id AS most_recently_modified_link_page_id,
       outdated.link_page_namespace AS most_recently_modified_link_page_namespace,
       outdated.link_page_title AS most_recently_modified_link_page_title,
       outdated.most_recent_link_modification_time AS most_recently_modified_link_update_time,
       MAX(outdated.outdatedness_in_seconds) AS outdatedness_in_seconds,
       t2.cat_pages AS num_pages_in_category
FROM
    categorylinks t1
JOIN
    category t2
ON t1.cl_to = t2.cat_title
JOIN
   (SELECT mru_link.pl_from,
           own_info.page_namespace AS pl_from_namespace,
           own_info.page_title AS pl_from_title,
           own_ts.last_modified,
           mru_link.link_page_id,
           mru_link.link_page_namespace,
           mru_link.link_page_title,
           mru_link.most_recent_link_modification_time,
           TIMESTAMPDIFF(SECOND, own_ts.last_modified, mru_link.most_recent_link_modification_time) AS outdatedness_in_seconds
    FROM
       (SELECT pagelinks.pl_from,
               link_info.page_id AS link_page_id,
               link_info.page_namespace AS link_page_namespace,
               link_info.page_title AS link_page_title,
               MAX(link_ts.last_modified) AS most_recent_link_modification_time
        FROM
            pagelinks
        JOIN
            page link_info
        ON pagelinks.pl_namespace = link_info.page_namespace AND pagelinks.pl_title = link_info.page_title
        JOIN
            pagetimestamps link_ts
        ON link_info.page_id = link_ts.page_id
        WHERE pagelinks.pl_from_namespace in (0, 1)
        GROUP BY pagelinks.pl_from) mru_link
    JOIN
        page own_info
    ON mru_link.pl_from = own_info.page_id
    JOIN
        pagetimestamps own_ts
    ON mru_link.pl_from = own_ts.page_id) outdated
ON t1.cl_from = outdated.pl_from
GROUP BY t1.cl_to
ORDER BY num_pages_in_category DESC

);
GO
