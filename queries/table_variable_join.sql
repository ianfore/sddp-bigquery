SELECT t.table_name, variable_id, v.table_version_id, v.table_name, variable_name, variable_type, column_name, column_type 
FROM s_variable v
left join s_table t 
on v.table_version_id = t.table_version_id
where t.table_name is null