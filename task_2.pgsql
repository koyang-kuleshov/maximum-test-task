WITH merged_table AS (
    SELECT
        c.communication_id AS communication_id,
        c.site_id AS site_id,
        c.visitor_id AS visitor_id,
        c.date_time AS communication_date_time,
        s.visitor_session_id AS visitor_session_id,
        LEAD(s.date_time) over(PARTITION BY c.communication_id ORDER BY(s.date_time)) AS session_date_time,
        s.campaign_id AS campaign_id,
        ROW_NUMBER() over(PARTITION BY c.communication_id ORDER BY (s.date_time)) AS row_n
    FROM
        communications AS c
    LEFT JOIN
        sessions AS s
    ON
        c.visitor_id = s.visitor_id
    WHERE
        c.site_id = s.site_id AND
        c.date_time > s.date_time
    ORDER BY
        communication_id
    )

SELECT
    mt.communication_id,
    mt.site_id,
    mt.visitor_id,
    mt.communication_date_time,
    CASE
        WHEN sr.session_date_time IS NOT NULL THEN mt.visitor_session_id
    END AS visitor_session_id,
    sr.session_date_time,
    CASE
        WHEN sr.session_date_time IS NOT NULL THEN mt.campaign_id
    END AS campaign_id,
    sr.max_n AS row_n
FROM
    merged_table AS mt
LEFT JOIN    
    (SELECT
    communication_id,
    MAX(row_n) AS max_n,
    MAX(session_date_time) AS session_date_time
    FROM
        merged_table
    GROUP BY
        communication_id
    ORDER BY
        communication_id
    ) AS sr
ON
    mt.communication_id = sr.communication_id
WHERE
    mt.row_n = sr.max_n