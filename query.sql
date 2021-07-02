WITH
  raw_data AS (
  SELECT
    *
  FROM
    `prj-pro-lz-ga360.204521949.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX = '20210616' ),
 
  customdimensions as (
  SELECT 
  fullVisitorId,
  visitStartTime, 
  customDimensions
  FROM raw_data),
  
  hits as (
  SELECT              
  fullVisitorId,
  visitStartTime,
  hits.hitnumber,
  hits.customDimensions
  FROM raw_data, unnest(hits) hits, unnest(hits.customDimensions) cd
  WHERE cd.index = 9 or cd.index = 10),
  
  new_customdimensions as (
  SELECT
  fullVisitorId,
  visitStartTime,
  ARRAY(
  SELECT AS STRUCT
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 7) AS `value`, 7 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(ht.customdimensions) where index = 9) AS `value`, 9 as index
  UNION ALL SELECT AS STRUCT
  (SELECT value FROM UNNEST(ht.customdimensions) where index = 10) AS `value`, 10 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 11) AS `value`, 11 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 21) AS `value`, 21 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 22) AS `value`, 22 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 23) AS `value`, 23 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 31) AS `value`, 31 as index
  UNION ALL SELECT AS STRUCT   
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 33) AS `value`, 33 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 37) AS `value`, 37 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 40) AS `value`, 40 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 41) AS `value`, 41 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 48) AS `value`, 48 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 62) AS `value`, 62 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 71) AS `value`, 71 as index
  UNION ALL SELECT AS STRUCT   
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 90) AS `value`, 90 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 91) AS `value`, 91 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 92) AS `value`, 92 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 93) AS `value`, 93 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 94) AS `value`, 94 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 102) AS `value`, 102 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 103) AS `value`, 103 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 106) AS `value`, 106 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 107) AS `value`, 107 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 108) AS `value`, 108 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 109) AS `value`, 109 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 110) AS `value`, 110 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 112) AS `value`, 112 as index
  UNION ALL SELECT AS STRUCT  
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 122) AS `value`, 122 as index
  UNION ALL SELECT AS STRUCT 
  (SELECT value FROM UNNEST(cd.customdimensions) where index = 125) AS `value`, 125 as index) as customDiemensions
  FROM 
  customdimensions cd
  INNER JOIN	
  hits ht
  USING 
  (fullVisitorId,visitStartTime))
  
  SELECT 
  visitorId,
  visitNumber,
  visitId,
  raw_data.visitStartTime,
  `date`,	
  totals, 
  trafficSource,
  device,
  geoNetwork,
  n_cd.customDiemensions,
  hits,
  raw_data.fullVisitorId,
  userId,
  clientId,
  channelGrouping,
  socialEngagementType
  FROM raw_data 
  INNER JOIN
  new_customdimensions n_cd
  USING 
  (fullVisitorId,visitStartTime)
