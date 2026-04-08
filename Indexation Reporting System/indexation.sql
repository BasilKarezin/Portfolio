WITH 
-- 1. Получаем последний ИП (Исполнительное Производство) с нужными условиями стоп-причин
CTE_LastIP AS (
    SELECT 
        contract_id,
        exec_process_closed_date,
        ip_nomer,
        ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY creation_date DESC, exec_process_inst_date DESC) as rn
    FROM contract_fssp_ip
    WHERE stop_reason LIKE '%47%1%1' OR stop_reason LIKE '%47%1%2'
),
LastIP AS (
    SELECT contract_id, exec_process_closed_date, ip_nomer
    FROM CTE_LastIP
    WHERE rn = 1
),

-- 2. Регион обслуживания
CTE_Region AS (
    SELECT cf.contract_id, fr.territory_of_service
    FROM contract_fssp cf
    JOIN fssp_reestr fr ON cf.fssp_reestr_id = fr.fssp_reestr_id
    GROUP BY cf.contract_id, fr.territory_of_service 
),

-- 3. Последний статус суда
CTE_LastCourtStatus AS (
    SELECT 
        cs.contract_id,
        cs.contract_court_status_id,
        cs.creation_date AS [дата статуса СУД],
        DATEADD(DAY, (DATEPART(DAY, CAST(cs.creation_date AS date)) - 1) * (-1), CAST(cs.creation_date AS date)) AS [Месяц статуса суд],
        cs.creation_by AS [автор суд],
        etap.contract_etap_type_name AS [этап СУД],
        stt.status_type_name AS [статус СУД],
        e.comment_text AS [Примечание СУД],
        cs.comment AS [Комментарий СУД],
        ROW_NUMBER() OVER (PARTITION BY cs.contract_id ORDER BY cs.creation_date DESC) as rn
    FROM contract_court_statuses cs
    JOIN status_types stt ON cs.court_status_id = stt.status_type_id
    LEFT JOIN etap_status_comments e ON e.etap_status_comment_id = cs.etap_status_comment_id
    LEFT JOIN contract_etap_types etap ON stt.contract_etap_type_id = etap.contract_etap_type_id
),
LastCourtStat AS (
    SELECT * FROM CTE_LastCourtStatus WHERE rn = 1
),

-- 4. Последний статус ФССП (исполнение)
CTE_LastFSSPStatus AS (
    SELECT 
        fst.contract_id,
        fst.executive_document_id,
        fst.creation_date,
        fst.fssp_status_name,
        fst.comment,
        etap.contract_etap_type_name AS etap,
        stt.status_type_name,
        ect.comment_text,
        ROW_NUMBER() OVER (PARTITION BY fst.executive_document_id ORDER BY fst.creation_date DESC) as rn
    FROM contract_fssp_statuses fst
    JOIN status_types stt ON fst.fssp_status_id = stt.status_type_id
    LEFT JOIN contract_etap_types etap ON stt.contract_etap_type_id = etap.contract_etap_type_id
    LEFT JOIN etap_status_comments ect ON ect.etap_status_comment_id = fst.etap_status_comment_id
),
LastFSSPStat AS (
    SELECT * FROM CTE_LastFSSPStatus WHERE rn = 1
),

-- 5. Даты событий ФССП
CTE_FSSP_Dates AS (
    SELECT 
        contract_id,
        executive_document_id,
        MAX(CASE WHEN fssp_status_id = ... THEN CAST(creation_date AS date) END) AS Date_... ,
        MAX(CASE WHEN fssp_status_id = ... THEN comment END) AS Comment_... ,
        MAX(CASE WHEN fssp_status_id = ... THEN CAST(creation_date AS date) END) AS Date_... 
    FROM contract_fssp_statuses
    WHERE fssp_status_id IN (... )
    GROUP BY contract_id, executive_document_id
),
Agg_FSSP_Dates AS (
    SELECT 
        contract_id,
        MAX(Date_... ) AS Date_GotByID,
        MAX(Comment_... ) AS Comment_BankName,
        MAX(Date_... ) AS Date_ReturnedByID
    FROM CTE_FSSP_Dates
    GROUP BY contract_id
),

-- 6. Документы индексации (сводная таблица)
CTE_IndexDocs AS (
    SELECT 
        contract_id,
        MAX(CASE WHEN document_type_id = 407 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 408 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 409 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 410 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 411 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 412 THEN FORMAT(creation_date, 'dd-MM-yyyy') END) AS Doc_... ,
        MAX(CASE WHEN document_type_id = 29 THEN CAST(creation_date AS date) END) AS Doc_29_Date
    FROM contract_documents
    WHERE document_type_id IN (... )
    GROUP BY contract_id
),

-- 7. Последний документ из списка (... )
CTE_LastSpecDoc AS (
    SELECT 
        d.contract_id,
        t.document_type_name,
        ROW_NUMBER() OVER (PARTITION BY d.contract_id ORDER BY d.creation_date DESC) as rn
    FROM contract_documents d
    JOIN document_types t ON t.document_type_id = d.document_type_id
    WHERE d.document_type_id IN (... )
),
LastSpecDoc AS (
    SELECT contract_id, document_type_name FROM CTE_LastSpecDoc WHERE rn = 1
),

-- 8. Проверка платежей ФССП
CTE_FSSP_Payments AS (
    SELECT DISTINCT contract_id FROM contract_payments WHERE source = 'ФССП'
),

-- 9. Проверка на индексацию в статусах суда
CTE_CourtIndexCheck AS (
    SELECT DISTINCT contract_id 
    FROM contract_court_statuses ccs
    LEFT JOIN etap_status_comments e ON e.etap_status_comment_id = ccs.etap_status_comment_id
    WHERE comment LIKE '%ИНДЕКСАЦИЯ%' 
       OR court_status_id IN (... )
),

-- 10. Владелец этапа (суд)
CTE_Owner AS (
    SELECT 
        e.contract_id,
        CASE 
            WHEN e.owner_id IN (... ) THEN '' 
            ELSE u.full_name 
        END AS full_name
    FROM contract_etaps e
    JOIN users u ON e.owner_id = u.user_id
    WHERE e.contract_etap_section_id = 3
),

-- 11. Дата последнего платежа
CTE_LastPayment AS (
    SELECT contract_id, MAX(payment_date) AS cpd
    FROM contract_payments
    GROUP BY contract_id
),

-- 12. Минимальная дата создания статуса ФССП
CTE_MinFSSPDate AS (
    SELECT cfs.contract_id, MIN(cfs.creation_date) AS min_crea
    FROM contract_fssp_statuses cfs
    LEFT JOIN (
        SELECT DISTINCT contract_id, creation_date, fssp_status_id 
        FROM contract_fssp_statuses
        WHERE fssp_status_id IN (... )
          AND creation_date < '2023-01-01'
    ) st_drop ON st_drop.contract_id = cfs.contract_id 
             AND st_drop.creation_date = cfs.creation_date 
             AND st_drop.fssp_status_id = cfs.fssp_status_id
    WHERE st_drop.contract_id IS NUL
      AND cfs.fssp_status_id IN (... )
    GROUP BY cfs.contract_id
),

-- 13. Проверка на смерть
CTE_Deceased AS (
    SELECT contract_id FROM contract_court_statuses st 
        JOIN status_types stt ON st.court_status_id = stt.status_type_id
        WHERE st.comment LIKE '%... %' OR st.comment LIKE '%... %' OR stt.status_type_name LIKE '%... %' OR stt.status_type_name LIKE '%... %'
    UNION
    SELECT contract_id FROM contract_fssp_statuses st 
        JOIN status_types stt ON st.fssp_status_id = stt.status_type_id
        WHERE st.comment LIKE '%... %' OR st.comment LIKE '%... %' OR stt.status_type_name LIKE '%... %' OR stt.status_type_name LIKE '%... %'
    UNION
    SELECT contract_id FROM contracts st 
        JOIN status_types stt ON st.status_id = stt.status_type_id
        WHERE st.status_comment LIKE '%... %' OR st.status_comment LIKE '%... %' OR stt.status_type_name LIKE '%... %' OR stt.status_type_name LIKE '%... %'
),

-- 14. Проблемные ИП
CTE_ProbIP AS (
    SELECT DISTINCT contract_id 
    FROM contract_fssp_ip 
    WHERE REPLACE(REPLACE([stop_reason], ' ', ''), '.', '') LIKE '%47%1%7%'
),

-- 15. Сумма индексации (касса)
CTE_IndexSum AS (
    SELECT ind.contract_id, SUM(cp.payment_amount) AS indsum
    FROM (
        SELECT contract_id, creation_date 
        FROM contract_court_statuses 
        WHERE (court_status_id = ...  AND comment LIKE '%ИНДЕКСАЦИЯ%') OR court_status_id = ... 
    ) ind
    JOIN contract_payments cp ON cp.contract_id = ind.contract_id AND cp.payment_date >= ind.creation_date
    GROUP BY ind.contract_id
),

-- 16. Надзорные производства
CTE_Supervisory AS (
    SELECT DISTINCT contract_id 
    FROM executive_documents 
    WHERE type_id = 11 OR case_number LIKE '%-н/%'
),

-- 17. ИД с максимальной суммой по контракту
CTE_MaxAmountED AS (
    SELECT 
        contract_id,
        executive_document_id,
        amount,
        creation_date,
        ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY amount DESC, creation_date DESC) as rn
    FROM executive_documents
    WHERE amount IS NOT NULL
),
LastMaxAmountED AS (
    SELECT 
        contract_id,
        executive_document_id,
        amount AS max_amount,
        creation_date AS date_last_id_max_amount
    FROM CTE_MaxAmountED
    WHERE rn = 1
)

-- ==========================================
-- ОСНОВНОЙ ЗАПРОС
-- ==========================================
SELECT DISTINCT 
    c.contract_id,
    c.contract_number,
    ... 
    c.ostatok_dolga,
    CONCAT(p.last_name, ' ', p.first_name, ' ', p.patronymic) [ФИО должника],
    p.birth_date [Дата рождения],
    p.birth_place [Место рождения должника],
    p.inn [ИНН (должника)],
    ed.case_number [Номер ИД],
    ed.amount [Сумма по ИД],
    (SELECT territory_of_service FROM fssp_reestr fr LEFT JOIN contract_fssp cf ON cf.fssp_reestr_id = fr.fssp_reestr_id WHERE cf.contract_id = c.contract_id) [Регион],
    crt_stat.*,
    fssp_stat.status_type_name [статус ИСПОЛ],
    CAST(fssp_stat.creation_date AS date) [дата статуса ИСПОЛ], 
    own_sud.full_name [Владелец ИСПОЛА],
    indkas.indsum [Касса],
    fd.Date_GotByID [Дата получения ИД банком - ИД предъявлен в стороннюю организацию],
    IIF(cp.cpd < fd.Date_GotByID, DATEADD(day, 92, fd.Date_GotByID), NULL) [Планируемая дата отзыва из банка], 
    fd.Date_ReturnedByID [Отметка о возврате ИД банком], 
    IIF(fp.contract_id IS NOT NULL, 'ФССП', 'Сам погасил') [Погашен/ФССП (сам или фссп)],
    fd.Comment_BankName [Наименование Банка (Комментарий из статуса иД пред в стор организацию)],
    idx.Doc_... _Date [Справка о погаш задолженности],
    cp.cpd [Дата последнего платежа],
    DATEDIFF(day, cp.cpd, GETDATE()) [дн с послед пл],
    lsdoc.document_type_name [Послед док на инд],
    IIF(cix.contract_id IS NOT NULL, 1, 0) [Начала ли работу судебка по Индексации],
    idx.Doc_...  [Заявление на индексацию],
    idx.Doc_... [Определение о возврате заявления об индексации],
    idx.Doc_...  [Определение об оставлении заявления об индексации без движения],
    idx.Doc_...  [Определение об отказе в индексации],
    idx.Doc_...  [Определение об индексации],
    idx.Doc_...  [Определение об оставлении без рассмотрения заявления об индексации],
    cc.case_no, 
    cc.court_name, 
    cc.court_address, 
    cc.summa_index, 
    cc.last_payment_date, 
    cc.ispol_document_number, 
    cc.opredelenie_date, 
    cc.summa_id,
    lip.ip_nomer, 
    lip.exec_process_closed_date,
    -- === 2 НОВЫХ СТОЛБЦА ===
    cp.cpd [Дата последнего платежа (доп)],
    DATEDIFF(day, lmed.date_last_id_max_amount, GETDATE()) [Дней с даты ИД макс суммы]
    -- =======================

FROM contracts c 
-- Основные связи
LEFT JOIN cont_pers_dtl dtl ON dtl.contract_id = c.contract_id
LEFT JOIN persons p ON p.person_id = dtl.person_id
LEFT JOIN (SELECT * FROM contract_courts WHERE contract_court_type = 'index') cc ON c.contract_id = cc.contract_id
LEFT JOIN (SELECT * FROM executive_documents WHERE type_id = ... ) ed ON ed.contract_id = c.contract_id

-- Подключенные CTE
LEFT JOIN LastIP lip ON lip.contract_id = c.contract_id
LEFT JOIN CTE_Region reg ON reg.contract_id = c.contract_id
LEFT JOIN LastCourtStat crt_stat ON c.contract_id = crt_stat.contract_id
LEFT JOIN LastFSSPStat fssp_stat ON ed.executive_document_id = fssp_stat.executive_document_id
LEFT JOIN CTE_Owner own_sud ON c.contract_id = own_sud.contract_id
LEFT JOIN Agg_FSSP_Dates fd ON fd.contract_id = c.contract_id
LEFT JOIN CTE_LastPayment cp ON cp.contract_id = c.contract_id
LEFT JOIN CTE_MinFSSPDate min_creation ON min_creation.contract_id = c.contract_id
LEFT JOIN CTE_Deceased die_st ON die_st.contract_id = c.contract_id
LEFT JOIN CTE_ProbIP prb ON prb.contract_id = c.contract_id
LEFT JOIN bankrupt b ON b.person_id = dtl.person_id
LEFT JOIN CTE_IndexSum indkas ON indkas.contract_id = c.contract_id
LEFT JOIN CTE_Supervisory ispnad ON ispnad.contract_id = c.contract_id
LEFT JOIN CTE_FSSP_Payments fp ON fp.contract_id = c.contract_id
LEFT JOIN LastSpecDoc lsdoc ON lsdoc.contract_id = c.contract_id
LEFT JOIN CTE_IndexDocs idx ON idx.contract_id = c.contract_id
LEFT JOIN CTE_CourtIndexCheck cix ON cix.contract_id = c.contract_id
LEFT JOIN LastMaxAmountED lmed ON lmed.contract_id = c.contract_id

WHERE
    c.contract_type_id IN (... )
    AND c.tsessionariy = ... 
    AND c.ostatok_dolga <= 0
    AND c.contract_id NOT IN (SELECT contract_id FROM contract_assignment)
    AND c.contract_id NOT IN (SELECT contract_id FROM contract_grafik WHERE agreement_receipt_date IS NOT NULL)
    AND die_st.contract_id IS NULL 
    AND c.contract_id NOT IN (
        SELECT contract_id FROM contract_court_statuses ccs 
        LEFT JOIN etap_status_comments e ON e.etap_status_comment_id = ccs.etap_status_comment_id 
        WHERE comment_text LIKE '%Наследств%'
    )
    AND b.person_id IS NULL 
    AND prb.contract_id IS NULL
    AND c.sum_total_debt > 70000
    AND dtl.person_type_id = 1
    AND ispnad.contract_id IS NULL
;
