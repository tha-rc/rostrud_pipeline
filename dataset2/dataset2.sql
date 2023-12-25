CREATE TABLE IF NOT EXISTS project_trudvsem.dataset2 AS
	SELECT cv.id_candidate, --DISTINCT ON (id_candidate)
									id_cv,
									date_publish, -- Дата публикации
									date_creation, -- Дата создания
									--date_inactivation, -- Не содержит данных
									date_modify_inner_info, -- Внутренняя дата обновления содержимого
									date_last_updated, -- Дата xml файла
									salary, -- Зарплата
									responses, -- Количество записей в таблице с откликами
	CASE
		WHEN gender IS NULL THEN NULL
		WHEN gender = 'Женский' THEN 0
		WHEN gender = 'Мужской' THEN 1
		ELSE NULL
	END AS gender, -- Гендер
	CASE
		WHEN region_code IS NULL THEN NULL
		WHEN substring(region_code from 1 for 2) = 'Ал' THEN NULL
		WHEN substring(region_code from 1 for 2) = 'Бе' THEN NULL
		WHEN substring(region_code from 1 for 2) = 'са' THEN NULL
		WHEN substring(region_code from 1 for 2) = 'Ск' THEN NULL
		WHEN substring(region_code from 1 for 2) = 'Уф' THEN NULL
		ELSE substring(region_code from 1 for 2)::int
	END AS region_code, -- Код региона, первые две цифры в int
	CASE
		WHEN busy_type = 'Временная' THEN 1
		WHEN busy_type = 'Временная занятость' THEN 1
		WHEN busy_type = 'Занятость по совместительству' THEN 2
		WHEN busy_type = 'Полная занятость' THEN 3
		WHEN busy_type = 'Постоянная занятость' THEN 4
		WHEN busy_type = 'Сезонная' THEN 5
		WHEN busy_type = 'Сезонная занятость' THEN 5
		WHEN busy_type = 'Стажировка' THEN 6
		WHEN busy_type = 'Удаленная' THEN 7
		WHEN busy_type = 'Удалённая занятость' THEN 7
		WHEN busy_type = 'Частичная занятость' THEN 8
		ELSE NULL
	END AS busy_type, -- Тип занятости
	CASE
		WHEN education_type = 'Высшее' THEN 1
		WHEN education_type = 'Незаконченное высшее' THEN 2
		WHEN education_type = 'Неоконченное высшее' THEN 2
		WHEN education_type = 'Среднее' THEN 3
		WHEN education_type = 'Среднее профессиональное' THEN 3
		ELSE NULL
	END AS education_type, -- Тип образования
	--CASE
	--	WHEN industry_code = 'AccountingTaxesManagement' THEN 1
	--	WHEN industry_code = 'Agricultural' THEN 2
	--	WHEN industry_code = 'BuldindRealty' THEN 3
	--	WHEN industry_code = 'CareerBegin' THEN 4
	--	WHEN industry_code = 'ChemicalAndFuelIndustry' THEN 5
	--	WHEN industry_code = 'Communal' THEN 6
	--	WHEN industry_code = 'Consulting' THEN 7
	--	WHEN industry_code = 'Culture' THEN 8
	--	WHEN industry_code = 'DeskWork' THEN 9
	--	WHEN industry_code = 'Education' THEN 10
	--	WHEN industry_code = 'ElectricpowerIndustry' THEN 11
	--	WHEN industry_code = 'Finances' THEN 12
	--	WHEN industry_code = 'Food' THEN 13
	--	WHEN industry_code = 'Forest' THEN 14
	--	WHEN industry_code = 'HomePersonal' THEN 15
	--	WHEN industry_code = 'HumanRecruitment' THEN 16
	--	WHEN industry_code = 'IndustrialTrainingMasters' THEN 17
	--	WHEN industry_code = 'Industry' THEN 18
	--	WHEN industry_code = 'InformationTechnology' THEN 19
	--	WHEN industry_code = 'Jurisprudence' THEN 20
	--	WHEN industry_code = 'Logistic' THEN 21
	--	WHEN industry_code = 'Management' THEN 22
	--	WHEN industry_code = 'Marketing' THEN 23
	--	WHEN industry_code = 'MechanicalEngineering' THEN 24
	--	WHEN industry_code = 'Medicine' THEN 25
	--	WHEN industry_code = 'Metallurgy' THEN 26
	--	WHEN industry_code = 'NotQualification' THEN 27
	--	WHEN industry_code = 'Resources' THEN 28
	--	WHEN industry_code = 'Restaurants' THEN 29
	--	WHEN industry_code = 'RootLightIndustry' THEN 30
	--	WHEN industry_code = 'Safety' THEN 31
	--	WHEN industry_code = 'Sales' THEN 32
	--	WHEN industry_code = 'ServiceMaintenance' THEN 33
	--	WHEN industry_code = 'SportsFitnessBeautySalons' THEN 34
	--	WHEN industry_code = 'StateServices' THEN 35
	--	WHEN industry_code = 'Transport' THEN 36
	--	WHEN industry_code = 'WorkingSpecialties' THEN 37
	--	ELSE NULL
	--END AS industry_code,
    	LENGTH(add_certificates_modified) AS len_add_certificates_modified, -- Длины текстовых полей
    	LENGTH(skills) AS len_skills,
    	LENGTH(additional_skills) AS len_additional_skills,							
	   	LENGTH(other_info_modified) AS len_other_info_modified,
	CASE
         WHEN birthday_mistake > 0 THEN NULL
		 ELSE birthday
    END AS birthday, -- Год рождения
	CASE
         WHEN experience_mistake > 0 THEN NULL
		 ELSE experience
    END AS experience, -- Опыт работы в годах
    CASE
         WHEN POSITION('сгенерировано' IN other_info) > 0 THEN 1
		 ELSE 0
    END AS is_generated -- Отметка, что резюме сгенерировано на основе желаемой позиции
FROM project_trudvsem.curricula_vitae AS cv
		LEFT JOIN (SELECT id_candidate, COUNT(id_response) as responses
                   FROM project_trudvsem.responses
                   GROUP BY id_candidate) AS rsp
		ON cv.id_candidate = rsp.id_candidate
    WHERE country = 'Российская Федерация' AND 
          substring(region_code::text from 1 for 2) != '91' AND --КЛАДР Крым
          substring(region_code::text from 1 for 2) != '92' AND --КЛАДР Севастополь
          substring(region_code::text from 1 for 2) != '93' AND --КЛАДР Донецкая
          substring(region_code::text from 1 for 2) != '94' AND --КЛАДР Луганская
          date_publish >= '2019.01.01'
-- ORDER BY id_candidate, date_publish, date_modify_inner_info DESC
-- LIMIT 500