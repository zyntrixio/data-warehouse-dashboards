WITH api_stats AS (
  SELECT
    *
  FROM
    {{ref('src__api_stats')}}
  WHERE CHANNEL = 'LBG')
  
  , dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
    WHERE
        DATE >= (SELECT min(date(date_time)) FROM api_stats)
        AND DATE <= CURRENT_DATE()
  )
  
  ,aggregate AS (
    SELECT
        d.DATE
        ,a.RESPONSE_TIME
        ,count(a.*) AS TOTAL_CALL_COUNT
    FROM
        api_stats a
    LEFT JOIN
        dim_date d ON DATE(a.DATE_TIME) = d.DATE 
    GROUP BY
        d.DATE
        ,a.RESPONSE_TIME
  )
  
  select * 
  from aggregate