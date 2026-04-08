WITH contracti AS (
    SELECT 
        contract_id,
        contract_number,
        batch_code,
        registry [Номер цессии],
        contr_agent_id,
        -- ИСПРАВЛЕННЫЙ ФОРМАТ ДЛЯ EXCEL
        sum_total_debt [ОСЗ куплено],
        ostatok_dolga [Остаток долга],
        dolgovoi_schet [Долговой счет],
        sum_osnovn_dolg [Сумма основного долга],
        sum_procent [Сумма процентов],
        sum_shtraf [Сумма штрафов],
        sum_comissia [Сумма комиссии],
        CONVERT(VARCHAR, registry_start_date, 104) [Дата цессии],
        sum_gosposhlina [Сумма госпошлины],
        CASE WHEN ostatok_dolga < 1 THEN 'Погашен' ELSE '' END [Погашен],
        CONVERT(VARCHAR, initial_debt_date, 104) [Дата выдачи кредита],
        REPLACE(REPLACE(status_comment, CHAR(13), ''), CHAR(10), '') [коммент досудебки],
        status_id,
        initial_debt_date
    FROM contracts c
    WHERE contract_type_id = ... 
        AND c.tsessionariy = ... 
		and batch_code not like '...'
		and batch_code not like '...'
and batch_code <> '...'
),

dtl AS (
    SELECT contract_id, person_id, person_type_id
    FROM cont_pers_dtl 
    WHERE person_type_id = 1
),

perss AS (
    SELECT 
        d.contract_id,
        p.person_id,
        p.last_name + ' ' + p.first_name + ' ' + ISNULL(p.patronymic, '') [ФИО],
        CONVERT(VARCHAR, p.birth_date, 104) [ДР]
    FROM dtl d
    LEFT JOIN persons p ON d.person_id = p.person_id
),

contragenti AS (
    SELECT
        ag.contragent_name [Продавец],
        c.contract_id
    FROM contracti c
    LEFT JOIN contragents ag ON c.contr_agent_id = ag.contragent_id
),

vibor AS (
    SELECT 
        c.contract_id,
        CASE 
            WHEN batch_code = '...' THEN '...'
            
            ELSE 'Неизвестная цессия'
        END [Цессия_наш_код]
    FROM contracti c
),

cessia AS (
    SELECT *
    FROM vibor
    WHERE [Цессия_наш_код] IS NOT NULL
),

ochered AS (
    SELECT 
        contract_id, 
        [Очередь]
    FROM (
        SELECT 
            q.contract_id,
            ROW_NUMBER() OVER (PARTITION BY q.contract_id ORDER BY q.queue_id DESC) AS num,
            qt.queue_type_name [Очередь]
        FROM contracti c
        LEFT JOIN queues q ON q.contract_id = c.contract_id
        LEFT JOIN queue_types qt ON q.queue_type_id = qt.queue_type_id 
    ) AS quj 
    WHERE num = 1
),

court AS (
    SELECT 
        co.court_code,
        c.contract_id,
        co.court_name [СУД],
        cc.case_no [Номер дела],
		co.court_region_name [Регион суда],
		co.phone [Телефон суда],
        CONVERT(VARCHAR, cc.court_decision_reception_date, 104) [Дата выдачи ИД]
    FROM contracti c
    LEFT JOIN contract_courts cc ON c.contract_id = cc.contract_id
    LEFT JOIN courts co ON cc.court_code = co.court_code
    WHERE cc.contract_court_type = 'main'
),

dosuz AS (
    SELECT 
        c.contract_id, 
        CASE WHEN COUNT(cc.contract_court_id) > 0 THEN 1 ELSE 0 END [Досуживание]
    FROM contracti c
    LEFT JOIN contract_courts cc ON c.contract_id = cc.contract_id AND cc.contract_court_type = 'sub'
    GROUP BY c.contract_id
),

owner_fact AS (
    SELECT 
        c.contract_id,
        CASE 
            WHEN e.owner_id = 83 THEN ''
            WHEN e.owner_id = 105 THEN ''
            ELSE u.full_name 
        END [Владелец судебки]
    FROM contracti c
    LEFT JOIN contract_etaps e ON c.contract_id = e.contract_id AND e.contract_etap_section_id = 2
    LEFT JOIN users u ON e.owner_id = u.user_id
),

dosud_stat AS (
    SELECT 
        c.contract_id,
        st.status_type_name [статус досудебки],
        c.status_id,
        c.[коммент досудебки]
    FROM contracti c
    LEFT JOIN status_types st ON c.status_id = st.status_type_id
),

court_statuses AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY creation_date DESC, contract_court_status_id DESC) AS rn
    FROM contract_court_statuses
),

sud_statis AS (
    SELECT 
        cs.contract_id,
        cs.creation_by [автор суд],
        esc.comment_text [Примечание СУД],
        etap.contract_etap_type_name [этап СУД],
        st.status_type_name [статус СУД],
        cs.creation_date [дата статуса СУД],
        DATEDIFF(day, cs.creation_date, GETDATE()) [дней с суд.статуса],
        REPLACE(REPLACE(cs.comment, CHAR(13), ''), CHAR(10), '') [коммент суд],
        CONVERT(VARCHAR, cs.creation_date, 104) [Дата статуса(СУД)],
        CASE 
            WHEN DATEDIFF(day, cs.creation_date, GETDATE()) <= 25 THEN '1-25'
            WHEN DATEDIFF(day, cs.creation_date, GETDATE()) <= 50 THEN '26-50'
            WHEN DATEDIFF(day, cs.creation_date, GETDATE()) <= 90 THEN '51-90'
            ELSE '90+' 
        END [Диапазон],
        DATEADD(DAY, -(DAY(cs.creation_date) - 1), cs.creation_date) [Месяц статуса суд]
    FROM court_statuses cs
    LEFT JOIN status_types st ON cs.court_status_id = st.status_type_id
    LEFT JOIN etap_status_comments esc ON cs.etap_status_comment_id = esc.etap_status_comment_id
    LEFT JOIN contract_etap_types etap ON st.contract_etap_type_id = etap.contract_etap_type_id
    WHERE cs.rn = 1
),

fssp_statuses AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY creation_date DESC, contract_fssp_status_id DESC) AS rn
    FROM contract_fssp_statuses
),

isp_statis AS (
    SELECT
        fs.contract_id,
        CONVERT(VARCHAR, fs.creation_date, 104) [Дата статуса(ИСП)],
        DATEDIFF(day, fs.creation_date, GETDATE()) [дней с исп.статуса],
        fs.creation_by [сотрудник],
        etap.contract_etap_type_name [этап ИСП],
        st.status_type_name [статус ИСП],
        esc.comment_text [Примечание ИСП],
        fs.comment [Комментарий ИСП]
    FROM fssp_statuses fs
    LEFT JOIN status_types st ON fs.fssp_status_id = st.status_type_id
    LEFT JOIN etap_status_comments esc ON fs.etap_status_comment_id = esc.etap_status_comment_id
    LEFT JOIN contract_etap_types etap ON st.contract_etap_type_id = etap.contract_etap_type_id
    WHERE fs.rn = 1
),

ex_doc AS (
    SELECT 
        c.contract_id,
        -- ИСПРАВЛЕННЫЙ ФОРМАТ ДЛЯ EXCEL
        sum(ex.amount) [Сумма суммы по гл ИД],
        COUNT(ex.executive_document_id) [Кол-во гл. ИД]
    FROM contracti c
    LEFT JOIN executive_documents ex ON c.contract_id = ex.contract_id AND ex.main = 1
    GROUP BY c.contract_id
),

ex_doc_ekb AS (
    SELECT 
        c.contract_id,
        COUNT(ex.executive_document_id) [Кол-во гл. ИД в ЕКБ]
    FROM contracti c
    LEFT JOIN executive_documents ex ON c.contract_id = ex.contract_id AND ex.main = 1 AND ex.location_id = 1
    GROUP BY c.contract_id
),

avto AS (
    SELECT 
        c.contract_id,
        CASE WHEN COUNT(ca.contract_avto_id) > 0 THEN 1 ELSE 0 END [Наличие залога],
        MAX(ca.vin_avtomobilya) [VIN авто],
        MAX(ca.god_vypuska_avtomobilya) [Год выпуска авто],
        MAX(ca.model_avtomobilya) [Модель/марка авто],
        MAX(ca.dtp) [ДТП]
    FROM contracti c
    LEFT JOIN contract_avtos ca ON c.contract_id = ca.contract_id
    GROUP BY c.contract_id
),

vzisk AS (
    SELECT DISTINCT
        c.contract_id, 
        1 [Наличие обращение взыскания]
    FROM contracti c
    INNER JOIN executive_documents ex ON c.contract_id = ex.contract_id AND ex.type_id = 10
),

prod_pogash AS (
    SELECT 
        c.contract_id, 
        COUNT(ca.contract_id) [Переуступка]
    FROM contracti c
    LEFT JOIN contract_assignment ca ON c.contract_id = ca.contract_id AND ca.cess_date IS NOT NULL
    GROUP BY c.contract_id
),

nevozm AS (
    SELECT DISTINCT
        c.contract_id, 
        1 [работа невозможна]
    FROM contracti c
    INNER JOIN contract_court_statuses cs ON c.contract_id = cs.contract_id
    INNER JOIN status_types st ON cs.court_status_id = st.status_type_id 
    WHERE st.status_type_name IN ('Работа невозможна', 'Работа завершена')
),

umer AS (
    SELECT DISTINCT 
        c.contract_id, 
        1 [Умер]
    FROM contracti c
    WHERE EXISTS (
        SELECT 1 
        FROM (
            SELECT contract_id FROM contracts WHERE contract_id = c.contract_id AND (status_comment LIKE '%...%' OR status_comment LIKE '%...%')
            UNION ALL
            SELECT contract_id FROM contract_court_statuses WHERE contract_id = c.contract_id AND (comment LIKE '%...%' OR comment LIKE '%...%')
            UNION ALL
            SELECT contract_id FROM contract_fssp_statuses WHERE contract_id = c.contract_id AND (comment LIKE '%...%' OR comment LIKE '%...%')
        ) t
    )
),

bankrot AS (
    SELECT 
        c.contract_id,
        pb.casenumber [Номер дела банкрот],
        CASE 
            WHEN pb.completion_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.completion_date >= c.initial_debt_date) THEN 'Процедура банкротства завершена'
            WHEN pb.termination_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.termination_date >= c.initial_debt_date) THEN 'Процедура банкротства прекращена'
			WHEN pb.action_id = ... AND (c.initial_debt_date IS NULL OR pb.completion_date >= c.initial_debt_date) THEN 'Погашенный банкрот'
            WHEN pb.ri_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.ri_date >= c.initial_debt_date) THEN 'Реализация имущества'
            WHEN pb.rk_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.rk_date >= c.initial_debt_date) THEN 'Реструктуризация долгов'
            WHEN be.bankrupt_event_type_id = ... AND (c.initial_debt_date IS NULL OR be.event_date >= c.initial_debt_date) THEN 'Внесудебный банкрот завершен'
            WHEN (pb.casenumber IS NOT NULL and pb.casenumber <> 'Vnesudebnoe') AND 
                 (c.initial_debt_date IS NULL OR COALESCE(pb.ri_date, pb.rk_date, pb.completion_date, pb.termination_date) >= c.initial_debt_date) THEN 'Номер дела в реестре банкротства'
        END [Банкротство],
        CONVERT(VARCHAR, 
            CASE 
                WHEN pb.ri_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.ri_date >= c.initial_debt_date) THEN pb.ri_date
                WHEN pb.rk_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.rk_date >= c.initial_debt_date) THEN pb.rk_date
                WHEN pb.completion_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.completion_date >= c.initial_debt_date) THEN pb.completion_date
                WHEN pb.termination_date IS NOT NULL AND (c.initial_debt_date IS NULL OR pb.termination_date >= c.initial_debt_date) THEN pb.termination_date
                WHEN be.bankrupt_event_type_id = ... AND (c.initial_debt_date IS NULL OR be.event_date >= c.initial_debt_date) THEN be.event_date  -- Дата для внесудебных
                ELSE NULL
            END, 104
        ) [Дата банкротства],
        be.bankrupt_event_type_id [Код мероприятия]
    FROM contracti c
    LEFT JOIN dtl dt ON c.contract_id = dt.contract_id
    LEFT JOIN person_bankrupts pb ON dt.person_id = pb.person_id
    LEFT JOIN bankrupt_events be ON pb.person_bankrupt_id = be.person_bankrupt_id
    WHERE (pb.person_id IS NOT NULL 
        OR be.bankrupt_event_type_id = ...)
        -- Исключаем случаи банкротства до выдачи (только если дата выдачи известна)
        AND (
            c.initial_debt_date IS NULL  -- Если дата выдачи неизвестна - не исключаем
            OR (
                (pb.ri_date IS NULL OR pb.ri_date >= c.initial_debt_date)
                AND (pb.rk_date IS NULL OR pb.rk_date >= c.initial_debt_date)
                AND (pb.completion_date IS NULL OR pb.completion_date >= c.initial_debt_date)
                AND (pb.termination_date IS NULL OR pb.termination_date >= c.initial_debt_date)
                AND (be.event_date IS NULL OR be.event_date >= c.initial_debt_date)
            )
        )
),

rtk AS (
    SELECT 
        c.contract_id,
        u.full_name [Владелец дела],
        CONVERT(VARCHAR, pb.rtk_sending_date, 104) [Дата отправки заявления в РТК],
        CONVERT(VARCHAR, DATEADD(DAY, -(DAY(pb.rtk_sending_date) - 1), pb.rtk_sending_date), 104) [Месяц РТК]
    FROM contracti c
    LEFT JOIN dtl dt ON dt.contract_id = c.contract_id
    LEFT JOIN person_bankrupts pb ON dt.person_id = pb.person_id
    LEFT JOIN users u ON pb.owner_id = u.user_id
    WHERE c.contr_agent_id NOT IN (...)
        AND u.full_name IS NOT NULL
),

soglash AS (
    SELECT 
        c.contract_id, 
        1 [Соглашение подписано]
    FROM contracti c
    WHERE EXISTS (
        SELECT 1 
        FROM contract_grafik cg 
        WHERE cg.contract_id = c.contract_id AND cg.agreement_receipt_date IS NOT NULL
    )
),

indecs AS (
    SELECT 
        c.contract_id, 
        1 [Индексация]
    FROM contracti c
    WHERE EXISTS (
        SELECT 1 
        FROM contract_court_statuses cc 
        WHERE cc.contract_id = c.contract_id AND cc.comment LIKE '...'
    )
),

docs AS (
    SELECT 
        c.contract_id,
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Досье для суда],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Заяв на ЗС],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о ЗС],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о возвращ ЗЗС],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об отказе в ЗС],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об ост. б/д в ЗЗС],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Заяв СП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Заяв СП на остаток долга],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о возвращ ЗСП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об отказе в принят ЗСП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об отмене СП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Судебный приказ],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Исковое заявление],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [ИЗ на остаток долга],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [ИЗ при отмене СП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о возвращ ИЗ],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об отказе в принят ИЗ],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Решение суда],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Исполнительный лист],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Дубликат СП],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Дубликат ИЛ],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о выд дубл],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о возвр заявл выд дубл],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об отказе выд дубл],
        MAX(CASE WHEN document_type_id IN (... ) THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр об ост. без движ.],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Заяв о вкл в РТК],
        MAX(CASE WHEN document_type_id = ... THEN CONVERT(VARCHAR, creation_date, 104) END) [Опр о вкл в РТК],
        MAX(CASE WHEN document_type_id IN (... ) THEN CONVERT(VARCHAR, creation_date, 104) END) [Уведомл о призн банкрот]
    FROM contracti c
    LEFT JOIN contract_documents cd ON c.contract_id = cd.contract_id
    WHERE document_type_id IN (... )
    GROUP BY c.contract_id
),

glav_stat as(
    SELECT 
        contract_id,
        status_type_name as [Главный статус (Статус из списка выше)],
        CONVERT(VARCHAR, creation_date, 104) as [Дата главного статуса]
    FROM (
        SELECT 
            c.contract_id,
            st.status_type_name,
            cc.creation_date,
            ROW_NUMBER() OVER (
                PARTITION BY c.contract_id 
                ORDER BY cc.creation_date DESC
            ) as rn
        FROM contracti c
        LEFT JOIN contract_court_statuses cc ON c.contract_id = cc.contract_id
        LEFT JOIN status_types st ON cc.court_status_id = st.status_type_id
        WHERE st.status_type_name not in(
'... ',
'... ',
'... ',
'... ',
'... ',
'... ',
'... ',
'... ',
'... ',

'... ',
'... ',
'... ',
'',
'',
'',
'',
'',
'',
''
)
    ) t
    WHERE rn = 1
),

itog AS (
    SELECT 
        c.contract_id,
        c.contract_number,
        c.[Дата выдачи кредита],
        CONVERT(VARCHAR, CAST(GETDATE() AS DATE), 104) [Дата выгрузки],
        CONVERT(VARCHAR, DATEADD(DAY,(DATEPART(DAY,CAST(GETDATE() AS DATE))-1)*(-1),CAST(GETDATE() AS DATE)), 104) [Текущий месяц работы],
        p.person_id,
        p.ФИО,
        p.ДР,
        ce.Цессия_наш_код,
        c.[Номер цессии],
        c.[Дата цессии],
        ca.Продавец,
        c.[ОСЗ куплено],
        c.[Остаток долга],
        c.[Долговой счет],
        c.[Сумма основного долга],
        c.[Сумма процентов],
        c.[Сумма штрафов],
        c.[Сумма комиссии],
        c.[Сумма госпошлины],
        och.Очередь,
        dosud.[статус досудебки],
        dosud.[коммент досудебки],
        crt.СУД,
		crt.[Регион суда],
		crt.[Телефон суда],
        crt.[Номер дела],
        crt.[Дата выдачи ИД],
        own.[Владелец судебки],
		gs.[Главный статус (Статус из списка выше)],
		gs.[Дата главного статуса],
        ss.[этап СУД],
        ss.[статус СУД],
        ss.[Примечание СУД],
        ss.[коммент суд],
        ss.[автор суд],
        ss.[Дата статуса(СУД)],
        ss.[дней с суд.статуса],
        ss.Диапазон,
        ss.[Месяц статуса суд],
        iss.[этап ИСП],
        iss.[статус ИСП],
        iss.[Примечание ИСП],
        iss.[Комментарий ИСП],
        iss.[Дата статуса(ИСП)],
        iss.[дней с исп.статуса],
        ex.[Сумма суммы по гл ИД],
        ex.[Кол-во гл. ИД],
        exe.[Кол-во гл. ИД в ЕКБ],
        av.[Наличие залога],
        vz.[Наличие обращение взыскания],
        pp.[Переуступка],
        c.Погашен,
        nev.[работа невозможна],
        um.[Умер],
        bank.[Банкротство],
        bank.[Номер дела банкрот],
        bank.[Дата банкротства],
        r.[Дата отправки заявления в РТК],
        r.[Месяц РТК],
        r.[Владелец дела],
        sog.[Соглашение подписано],
        dz.Досуживание,
        ind.Индексация,
        doc.[Досье для суда],
        doc.[Заяв на ЗС],
        doc.[Опр о ЗС],
        doc.[Опр о возвращ ЗЗС],
        doc.[Опр об отказе в ЗС],
        doc.[Опр об ост. б/д в ЗЗС],
        doc.[Заяв СП],
        doc.[Заяв СП на остаток долга],
        doc.[Опр о возвращ ЗСП],
        doc.[Опр об отказе в принят ЗСП],
        doc.[Опр об отмене СП],
        doc.[Судебный приказ],
        doc.[Исковое заявление],
        doc.[ИЗ на остаток долга],
        doc.[ИЗ при отмене СП],
        doc.[Опр о возвращ ИЗ],
        doc.[Опр об отказе в принят ИЗ],
        doc.[Решение суда],
        doc.[Исполнительный лист],
        doc.[Дубликат СП],
        doc.[Дубликат ИЛ],
        doc.[Опр о выд дубл],
        doc.[Опр о возвр заявл выд дубл],
        doc.[Опр об отказе выд дубл],
        doc.[Опр об ост. без движ.],
        doc.[Заяв о вкл в РТК],
		doc.[Опр о вкл в РТК],
        doc.[Уведомл о призн банкрот],
        av.[VIN авто],
        av.[Год выпуска авто],
        av.[Модель/марка авто],
        av.ДТП,
        ROW_NUMBER() OVER (PARTITION BY c.contract_id ORDER BY c.contract_id) AS rn
    FROM contracti c
    LEFT JOIN perss p ON c.contract_id = p.contract_id
    LEFT JOIN contragenti ca ON c.contract_id = ca.contract_id
    LEFT JOIN cessia ce ON c.contract_id = ce.contract_id
    LEFT JOIN ochered och ON c.contract_id = och.contract_id
    LEFT JOIN court crt ON c.contract_id = crt.contract_id
    LEFT JOIN owner_fact own ON c.contract_id = own.contract_id
    LEFT JOIN dosud_stat dosud ON c.contract_id = dosud.contract_id
    LEFT JOIN sud_statis ss ON c.contract_id = ss.contract_id
    LEFT JOIN isp_statis iss ON c.contract_id = iss.contract_id
    LEFT JOIN ex_doc ex ON c.contract_id = ex.contract_id
    LEFT JOIN ex_doc_ekb exe ON c.contract_id = exe.contract_id
    LEFT JOIN avto av ON c.contract_id = av.contract_id
    LEFT JOIN vzisk vz ON c.contract_id = vz.contract_id
    LEFT JOIN prod_pogash pp ON c.contract_id = pp.contract_id
    LEFT JOIN nevozm nev ON c.contract_id = nev.contract_id
    LEFT JOIN umer um ON c.contract_id = um.contract_id
    LEFT JOIN bankrot bank ON c.contract_id = bank.contract_id
    LEFT JOIN rtk r ON c.contract_id = r.contract_id
    LEFT JOIN soglash sog ON c.contract_id = sog.contract_id
    LEFT JOIN indecs ind ON c.contract_id = ind.contract_id
    LEFT JOIN docs doc ON c.contract_id = doc.contract_id
    LEFT JOIN dosuz dz ON c.contract_id = dz.contract_id
	LEFT JOIN glav_stat gs on c.contract_id = gs.contract_id
)

SELECT 
    i.*,
    CASE 
        -- 1)Условие для погашенных договоров (первое и главное)
        WHEN i.[Погашен] = ... 
        OR i.[статус ИСП] IN ... ... 
        OR i.[статус СУД] IN ... ... ... 
		or i.[Банкротство] = ... ... ... 
    THEN 'Работа завершена'

	--2)РТК
        when [статус СУД] ='... ' 
		and [Дата отправки заявления в РТК]  is null
	then 'Принято в работу'


	when [статус СУД] ='... '
	and DATEADD(DAY,(DATEPART(DAY,TRY_CONVERT(DATE, [Дата отправки заявления в РТК], 104))-1)*(-1),TRY_CONVERT(DATE, [Дата отправки заявления в РТК], 104))<>i.[Текущий месяц работы]
	then 'Отработано'

	---3)Работа невозможна
	when [статус СУД] in 
		('... '
		) or [статус СУД] is null
	then 'Сегментация'

when [статус СУД] in 
		('... ') 
		and [дней с суд.статуса]<=45
	then 'Отработано'

        -- 4)Логика банкротства
         WHEN i.[Банкротство] IN ('... ', '... ') 
    THEN 'Банкротство'

        ---5)Работа невозможна была здесь, перенес выше
        when [статус СУД] in 
		(
		... ... )
	then 'Сегментация'


	when [статус СУД] in 
		('... ',
		'... ',
		... ) 
	then 'Результат получен'
	when [статус СУД] in 
		(... 
		)
		or ([статус СУД]  ='... ' and [Примечание СУД] is not null)
	then 'Принято в работу'
	when [статус СУД] = '... '
	then 'Передан на ГП'
	when [статус СУД] in 
		('... '
		)
	then 'Обратная связь'
	when [статус СУД] = '... '
	then 'Направлено сторонам'
	when [статус СУД] in 
		(... )
	then 'Госпошлина оплачена'

	when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]<=60
	then 'Отработано'
		when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]>60
	then 'Направить запрос'
	when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]<=90
	then 'Отработано'
		when [статус СУД] in 
		(... 
) 
		and [дней с суд.статуса]>90
	then 'Направить запрос'
	when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]<=150
	then 'Отработано'
		when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]>150
	then 'Направить запрос'

	when [статус СУД] in 
		(... ) 
		and  [дней с суд.статуса]<=45
	then 'Отработано'
		when [статус СУД] in 
		(... )  
		and  [дней с суд.статуса]>45
	then 'Направить запрос'		

		when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]<=60
	then 'Отработано'
		when [статус СУД] in 
		(... )
		and  [дней с суд.статуса]>60
	then 'Направить запрос'	
	
		when [статус СУД] in 
		(... ) 
		and [дней с суд.статуса]<=90
	then 'Отработано'
		when [статус СУД] in 
		(... )
		and  [дней с суд.статуса]>90
	then 'Направить запрос'	
		
		when [статус СУД] ='... '
		and  [дней с суд.статуса]<=60
	then 'Отработано'	
	when [статус СУД] ='... '
		and  [дней с суд.статуса]>60
	then 'Направить запрос'	
	when [статус СУД] ='... '
		and  [дней с суд.статуса]<=60
	then 'Отработано'	
	when [статус СУД] ='... '
		and  [дней с суд.статуса]>60
	then 'Направить запрос'	


	when [статус СУД] in (... )
		and  [дней с суд.статуса]<=60
	then 'Результат получен'	
	when [статус СУД] in (... )
		and  [дней с суд.статуса]>60
	then 'Направить запрос'	
	else 'Пусто'
    END [Группа]

FROM itog i
WHERE i.rn = 1
  AND (i.Переуступка = 0 OR i.Переуступка IS NULL)
  AND i.Продавец <> 'test1'
    --and contract_id in(... )
ORDER BY i.contract_id
