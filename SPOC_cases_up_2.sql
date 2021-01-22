SELECT 
    cas.id as case_id,
    cas.name,
    agt.first_name as agent_name,
    cas.account_id,
    cstm.disrupt_user_id_c as user_id,
    cas.date_entered,
    DATE_TRUNC('day',cas.date_entered)::DATE as day,
    cas.date_modified,
    cas.resolution,
    cas.state
FROM spoc_ro.cases_cur as cas 
LEFT JOIN spoc_ro.accounts_cstm_cur as cstm
ON cas.account_id = cstm.id_c
LEFT JOIN prolix.donna_agentuser_cur agt
ON cas.assigned_user_id = agt.agent_uuid 
WHERE name NOT IN ('Collections') AND day >= DATEADD(day,-7, (SELECT getdate() as date))