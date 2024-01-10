CREATE TABLE IF NOT EXISTS project_trudvsem.agg_edu AS
	SELECT DISTINCT ON (cv.id_cv) cv.id_cv, 
						CONCAT('[', STRING_AGG(edu, ','), ']') AS edu, 
						CONCAT('[', STRING_AGG(addedu, ','), ']') AS addedu, 
						(LENGTH(CONCAT('[', STRING_AGG(edu, ','), ']')) + LENGTH(CONCAT('[', STRING_AGG(addedu, ','), ']'))) AS completeness_score
                FROM project_trudvsem.curricula_vitae AS cv
                  LEFT JOIN (SELECT id_cv, CONCAT('{', '"faculty":"', COALESCE(REPLACE(faculty::text, '"', ''), ''), '"', ',',  '"graduate_year":"', COALESCE(REPLACE(graduate_year::text, '"', ''), ''), '"', ',',  '"legal_name":"', COALESCE(REPLACE(legal_name::text, '"', ''), ''), '"', ',',  '"qualification":"', COALESCE(REPLACE(qualification::text, '"', ''), ''), '"', ',',  '"speciality":"', COALESCE(REPLACE(speciality::text, '"', ''), ''), '"', '}') AS edu
                             FROM (SELECT DISTINCT id_cv, faculty, graduate_year, legal_name, qualification, speciality
									FROM project_trudvsem.edu
									WHERE grad_year_mistake = 0 AND id_cv IS NOT NULL AND LENGTH(COALESCE(faculty, graduate_year::text, legal_name, qualification, '')) > 3) AS e) AS e
                       ON cv.id_cv = e.id_cv
                    LEFT JOIN (SELECT id_cv, CONCAT('{', '"course_name":"', COALESCE(REPLACE(course_name::text, '"', ''), ''), '"', ',',  '"description":"', COALESCE(REPLACE(description::text, '"', ''), ''), '"', ',',  '"graduate_year":"', COALESCE(REPLACE(graduate_year::text, '"', ''), ''), '"', ',',  '"legal_name":"', COALESCE(REPLACE(legal_name::text, '"', ''), ''), '"', '}') AS addedu
                              FROM (SELECT DISTINCT id_cv, course_name, description, graduate_year, legal_name    
									 FROM project_trudvsem.addedu
									 WHERE grad_year_mistake = 0 AND id_cv IS NOT NULL AND LENGTH(COALESCE(course_name, description, graduate_year::text, legal_name, '')) > 3) AS ae) AS ae
                        ON cv.id_cv = ae.id_cv
        GROUP BY cv.id_cv, date_last_updated, cv.date_modify_inner_info, date_publish
        ORDER BY cv.id_cv, date_last_updated, cv.date_modify_inner_info, date_publish DESC;

CREATE TABLE IF NOT EXISTS project_trudvsem.agg_exp AS
	SELECT DISTINCT ON (cv.id_cv) cv.id_cv, 
				CONCAT('[', STRING_AGG(workexp, ','), ']') AS workexp, 
				LENGTH(CONCAT('[', STRING_AGG(workexp, ','), ']')) AS completeness_score
                FROM project_trudvsem.curricula_vitae AS cv
                  LEFT JOIN (SELECT id_cv, CONCAT('{', '"company_name":"', COALESCE(REPLACE(company_name::text, '"', ''), ''), '"', ',',  '"date_from":"', COALESCE(REPLACE(date_from::text, '"', ''), ''), '"', ',',  '"date_to":"', COALESCE(REPLACE(date_to::text, '"', ''), ''), '"', ',', '"job_title":"', COALESCE(REPLACE(job_title::text, '"', ''), ''), '"', '}') AS workexp
                             FROM (SELECT DISTINCT id_cv, company_name, date_from, date_to, job_title
									FROM project_trudvsem.workexp
									WHERE date_mistake = 0 AND id_cv IS NOT NULL AND LENGTH(COALESCE(company_name, date_from::text, date_to::text, job_title, '')) > 3) AS exp) AS exp
                       ON cv.id_cv = exp.id_cv
        GROUP BY cv.id_cv, date_last_updated, cv.date_modify_inner_info, date_publish
        ORDER BY cv.id_cv, date_last_updated, cv.date_modify_inner_info, date_publish DESC;

CREATE TABLE IF NOT EXISTS project_trudvsem.dataset1 AS
	SELECT DISTINCT ON (id_candidate, cv.id_cv)
                                    id_candidate,
									cv.id_cv,
                                    ed.edu,
                                    ed.addedu,
                                    ex.workexp,
									--date_publish, -- Дата публикации
									--date_creation, -- Дата создания
									--date_inactivation, -- Не содержит данных
									date_modify_inner_info, -- Внутренняя дата обновления содержимого
									--date_last_updated, -- Дата xml файла
                                    position_name,
									salary, -- Зарплата
        CASE
            WHEN gender IS NULL THEN NULL
            WHEN gender = 'Женский' THEN 0
            WHEN gender = 'Мужской' THEN 1
            ELSE NULL
        END AS gender, -- Гендер
        CASE
            WHEN birthday_mistake > 0 THEN NULL
            ELSE birthday
        END AS birthday, -- Год рождения
        CASE
            WHEN region_code IS NULL THEN NULL
            WHEN substring(region_code from 1 for 2) = 'Ал' THEN NULL
            WHEN substring(region_code from 1 for 2) = 'Бе' THEN NULL
            WHEN substring(region_code from 1 for 2) = 'са' THEN NULL
            WHEN substring(region_code from 1 for 2) = 'Ск' THEN NULL
            WHEN substring(region_code from 1 for 2) = 'Уф' THEN NULL
            ELSE substring(region_code from 1 for 2)::int
        END AS region_code -- Код региона, первые две цифры в int
    FROM project_trudvsem.curricula_vitae AS cv
        LEFT JOIN project_trudvsem.agg_edu as ed
            ON cv.id_cv = ed.id_cv
        LEFT JOIN project_trudvsem.agg_exp as ex
            ON cv.id_cv = ex.id_cv
    WHERE country = 'Российская Федерация' AND 
          substring(region_code::text from 1 for 2) != '91' AND --КЛАДР Крым
          substring(region_code::text from 1 for 2) != '92' AND --КЛАДР Севастополь
          substring(region_code::text from 1 for 2) != '93' AND --КЛАДР Донецкая
          substring(region_code::text from 1 for 2) != '94' --КЛАДР Луганская
          -- date_publish >= '2019.01.01'
    ORDER BY id_candidate, cv.id_cv, date_last_updated, date_modify_inner_info, date_creation, date_publish DESC;