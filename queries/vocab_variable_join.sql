
SELECT vocab_id, vocab_name , s.*, t.table_name, st.*
FROM vocab v
LEFT JOIN s_variable AS s on s.variable_id = v.vocab_name
left join s_table as t on t.table_version_id = s.table_version_id
left join s_study as st on st.study_version_id = t.study_version_id
order by study_name

