-- TimeDifference Function
-- 

SELECT timediff (hours, 
          cast(CONVERT_TIMEZONE( 
                                case   when From_TZ = 1 then 'EST'
                                       when From_TZ = 2 then 'CET'
                                       when From_TZ = 3 then 'GMT'
                                       when From_TZ = 4 then 'UTC'
                                       when From_TZ = 5 then 'ACT'
                                       when From_TZ = 6 then 'EET'
                                       when From_TZ = 10 then 'America/Indiana/Knox'
                                       when From_TZ = 10 then 'PST'
                                       when From_TZ = 10 then 'MST'
                                       else NULL
                                end, CTime) as timestamp_NTZ) ,
          cast( CONVERT_TIMEZONE( 
                                case   when To_TZ = 1 then 'EST'
                                       when To_TZ = 2 then 'CET'
                                       when To_TZ = 3 then 'GMT'
                                       when To_TZ = 4 then 'UTC'
                                       when To_TZ = 5 then 'ACT'
                                       when To_TZ = 6 then 'EET'
                                       when To_TZ = 10 then 'America/Indiana/Knox'
                                       when To_TZ = 10 then 'PST'
                                       when To_TZ = 10 then 'MST'
                                       else NULL
                                end, CTime) as timestamp_NTZ) 
         )
