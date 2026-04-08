WITH 
-- Создаем временную таблицу с ключевыми словами для поиска (нормализованными)
keywords AS (
    SELECT ... AS keyword UNION
    SELECT ... ... UNION
    SELECT ... ... UNION
    SELECT ... UNION
    SELECT ... ... UNION
    SELECT ... ... ... UNION
    SELECT ... ... ... UNION
    SELECT ... ... ... UNION
    SELECT ... ... ... UNION
    SELECT ... ... 
),
-- Получаем всех кредиторов с их данными и флагами наличия ключевых слов
bankrupt_creditors AS (
    SELECT 
        bw.person_bankrupt_id,
        bw.bankrupt_work_creditor,
        -- Нормализуем для поиска
        LOWER(REPLACE(REPLACE(REPLACE(REPLACE(
            ISNULL(bw.bankrupt_work_creditor, ''), 
            '«', ''), '»', ''), '"', ''), ' ', '')) AS normalized_creditor,
        -- Проверяем наличие наших ключевых слов
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM keywords k 
                WHERE LOWER(bw.bankrupt_work_creditor) LIKE '%' + k.keyword + '%'
            ) THEN 1 ELSE 0 
        END AS contains_our_keyword,
        bw.bankrupt_work_sum,
        ROW_NUMBER() OVER (
            PARTITION BY bw.person_bankrupt_id 
            ORDER BY 
                -- Сначала кредиторы с нашими ключевыми словами
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM keywords k 
                        WHERE LOWER(bw.bankrupt_work_creditor) LIKE '%' + k.keyword + '%'
                    ) THEN 0 ELSE 1 
                END,
                -- Потом по сумме (наибольшая)
                ISNULL(bw.bankrupt_work_sum, 0) DESC,
                -- И по ID для детерминированности
                bw.bankrupt_work_id
        ) AS rn
    FROM bankrupt_work bw
),
-- Основная нормализация контрагентов
normalized_contragents AS (
    SELECT 
        c.contract_id,
        ca.contragent_name AS original_cedent,
        -- Нормализуем цедента для поиска
        LOWER(REPLACE(REPLACE(REPLACE(REPLACE(
            ISNULL(ca.contragent_name, ''), 
            '«', ''), '»', ''), '"', ''), ' ', '')) AS simple_normalized_cedent,
        -- Проверяем наличие наших ключевых слов в цеденте
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM keywords k 
                WHERE LOWER(ca.contragent_name) LIKE '%' + k.keyword + '%'
            ) THEN 1 ELSE 0 
        END AS cedent_contains_our_keyword,
        c.registry,
        c.contract_number,
        c.registry_start_date,
        c.initial_debt_date,
        c.sum_total_debt,
        c.ostatok_dolga,
        (SELECT MAX(completion_date) 
         FROM person_bankrupts pb2 
         WHERE pb2.person_id = cp.person_id) AS completion_date,
        (SELECT MAX(event_date) 
         FROM bankrupt_events be2 
         WHERE be2.person_bankrupt_id = pb.person_bankrupt_id 
           AND be2.bankrupt_event_type_id = 7) AS event_date,
        MAX(CASE WHEN be.bankrupt_event_type_id = 7 THEN 1 ELSE 0 END) AS has_event_type_7,
        pb.casenumber,
        pb.person_bankrupt_id,
        COUNT(DISTINCT bw.bankrupt_work_id) AS bankrupt_work_count,
        pb.collection_result_id,
        -- Получаем информацию о приоритетном кредиторе
        MAX(bc.contains_our_keyword) AS has_bankrupt_with_our_keyword,
        -- Для отладки
        COUNT(DISTINCT CASE WHEN bc.contains_our_keyword = 1 THEN bc.bankrupt_work_creditor END) AS count_our_keyword_creditors
    FROM contracts c
    INNER JOIN cont_pers_dtl cp ON c.contract_id = cp.contract_id
    INNER JOIN person_bankrupts pb ON cp.person_id = pb.person_id
    LEFT JOIN contragents ca ON c.contr_agent_id = ca.contragent_id
    LEFT JOIN bankrupt_work bw ON pb.person_bankrupt_id = bw.person_bankrupt_id
    LEFT JOIN bankrupt_creditors bc ON pb.person_bankrupt_id = bc.person_bankrupt_id AND bc.rn = 1
    LEFT JOIN bankrupt_events be ON pb.person_bankrupt_id = be.person_bankrupt_id
    WHERE c.ostatok_dolga > 0 
        AND c.tsessionariy = ...  
        AND c.contract_type_id = ...  
        AND (pb.completion_date IS NOT NULL OR EXISTS (
            SELECT 1 FROM bankrupt_events be2 
            WHERE be2.person_bankrupt_id = pb.person_bankrupt_id 
            AND be2.bankrupt_event_type_id = ... 
        ))
        AND NOT EXISTS (
            SELECT 1 
            FROM bankrupt_events be3 
            WHERE be3.person_bankrupt_id = pb.person_bankrupt_id
            AND be3.bankrupt_event_type_id IN (... )
        )
    GROUP BY 
        c.contract_id,
        ca.contragent_name,
        c.registry,
        c.contract_number,
        c.registry_start_date,
        c.initial_debt_date,
        c.sum_total_debt,
        c.ostatok_dolga,
        pb.casenumber,
        pb.person_bankrupt_id,
        cp.person_id,
        pb.collection_result_id
),
-- Определяем приоритет на основе наличия наших ключевых слов
ranked_contragents AS (
    SELECT 
        nc.*,
        -- Получаем информацию о приоритетном кредиторе для финальной выборки
        (
            SELECT TOP 1 
                bc.bankrupt_work_creditor + 
                CASE 
                    WHEN bc.contains_our_keyword = 1 THEN ' (наше юрлицо)'
                    ELSE ''
                END
            FROM bankrupt_creditors bc 
            WHERE bc.person_bankrupt_id = nc.person_bankrupt_id
            ORDER BY 
                bc.contains_our_keyword DESC,
                ISNULL(bc.bankrupt_work_sum, 0) DESC,
                bc.bankrupt_work_creditor
        ) AS selected_bankrupt_creditor,
        (
            SELECT TOP 1 
                bc.bankrupt_work_sum
            FROM bankrupt_creditors bc 
            WHERE bc.person_bankrupt_id = nc.person_bankrupt_id
            ORDER BY 
                bc.contains_our_keyword DESC,
                ISNULL(bc.bankrupt_work_sum, 0) DESC
        ) AS selected_bankrupt_sum,
        (
            SELECT TOP 1 
                bc.contains_our_keyword
            FROM bankrupt_creditors bc 
            WHERE bc.person_bankrupt_id = nc.person_bankrupt_id
            ORDER BY 
                bc.contains_our_keyword DESC,
                ISNULL(bc.bankrupt_work_sum, 0) DESC
        ) AS selected_contains_keyword,
        -- Приоритет для сортировки
        CASE 
            WHEN nc.cedent_contains_our_keyword = 1 AND 
                 EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id AND bc.contains_our_keyword = 1) THEN 1
            WHEN EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id AND bc.contains_our_keyword = 1) THEN 2
            WHEN nc.cedent_contains_our_keyword = 1 THEN 3
            WHEN EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) AND 
                 nc.simple_normalized_cedent = (SELECT TOP 1 normalized_creditor FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) THEN 4
            WHEN NOT EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) THEN 5
            ELSE 6
        END AS priority_order,
        ROW_NUMBER() OVER (
            PARTITION BY nc.contract_id 
            ORDER BY 
                CASE 
                    WHEN nc.cedent_contains_our_keyword = 1 AND 
                         EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id AND bc.contains_our_keyword = 1) THEN 1
                    WHEN EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id AND bc.contains_our_keyword = 1) THEN 2
                    WHEN nc.cedent_contains_our_keyword = 1 THEN 3
                    WHEN EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) AND 
                         nc.simple_normalized_cedent = (SELECT TOP 1 normalized_creditor FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) THEN 4
                    WHEN NOT EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = nc.person_bankrupt_id) THEN 5
                    ELSE 6
                END,
                nc.bankrupt_work_count DESC
        ) AS rn
    FROM normalized_contragents nc
)
-- Финальный SELECT
SELECT 
    rc.contract_id, 
    CASE 
        WHEN rc.completion_date IS NOT NULL THEN rc.completion_date 
        ELSE CAST(rc.event_date AS DATE)
    END AS 'Дата банкротства',
    'Списание банкротов' AS 'Тип', 
    'Робот ЕФРСБ' AS 'Комментарий',
    rc.registry, 
    rc.original_cedent AS 'Изначальный цедент',
    rc.selected_bankrupt_creditor AS 'Заявленный цедент',
    rc.contract_number, 
    rc.registry_start_date AS 'Дата цессии', 
    rc.initial_debt_date AS 'Дата выдачи',
    rc.sum_total_debt AS 'OSZ',
    rc.ostatok_dolga AS 'ostatok',
    CASE 
        WHEN rc.collection_result_id = 3 THEN 'Освобожден'
        WHEN rc.casenumber LIKE '%nesudebn%' OR rc.casenumber IS NULL THEN 'Освобожден'
        WHEN rc.collection_result_id IS NULL THEN 'Освобожден'
        ELSE 'Проверить освобождение'
    END AS 'Проверка обязательств',
    rc.selected_bankrupt_sum AS 'Заявленная сумма',
    CASE 
        WHEN rc.contract_id IN (
            SELECT contract_id 
            FROM contract_assignment 
            WHERE assignment_type <> 'Списание банкротов'
        ) THEN 'Переуступка' 
        ELSE '' 
    END AS 'Переуступка',
    CASE 
        WHEN rc.contract_id IN (
            SELECT contract_id 
            FROM contract_grafik 
            WHERE agreement_receipt_date IS NOT NULL
        ) THEN 1 
        ELSE 0 
    END AS 'Получение соглашение',
    CASE 
        WHEN cv.contract_id IS NOT NULL THEN 'Zalog_avto' 
        ELSE '' 
    END AS Zalog,
    -- Логика проверки с акцентом на наши ключевые слова
    CASE 
        WHEN rc.has_event_type_7 = 1 THEN 
            CASE 
                WHEN cv.contract_id IS NOT NULL THEN 'Внесудебный залог'
                WHEN rc.selected_contains_keyword = 1 THEN 'Можно списывать (наше юрлицо в заявленном)'
                ELSE 'Можно списывать'
            END
        WHEN rc.casenumber IS NOT NULL AND rc.casenumber <> '' AND rc.casenumber <> 'Vnesudebne' THEN
            CASE 
                WHEN cv.contract_id IS NOT NULL THEN 'Залог, не проверен'
                WHEN rc.initial_debt_date > rc.completion_date THEN 'Банкрот до выдачи, не списываем'
                WHEN rc.registry_start_date > rc.completion_date THEN 'Банкрот до цессии'
                WHEN rc.selected_contains_keyword = 1 THEN 'Можно списывать (наше юрлицо в заявленном)'
                WHEN rc.cedent_contains_our_keyword = 1 THEN 'Цедент наш, но кредитор не наш - проверить'
                ELSE 'Требует проверки (не наши ключевые слова)'
            END
        ELSE 'Требует проверки'
    END AS 'Проверка',
    -- Разделили на два отдельных столбца
    CASE 
        WHEN rc.has_event_type_7 = 1 THEN 'Внесудебный'
        ELSE 'Судебный'
    END AS 'Суд/внесуд',
    -- Отдельный столбец для номера дела
    CASE 
        WHEN rc.has_event_type_7 = 1 THEN NULL
        ELSE rc.casenumber
    END AS 'Номер дела',
    -- Детальная информация о наличии наших ключевых слов
    CASE 
        WHEN rc.selected_contains_keyword = 1 AND rc.cedent_contains_our_keyword = 1 
            THEN 'Наши ключевые слова в обоих полях'
        WHEN rc.selected_contains_keyword = 1 
            THEN 'Наши ключевые слова только в заявленном'
        WHEN rc.cedent_contains_our_keyword = 1 
            THEN 'Наши ключевые слова только в цеденте'
        WHEN EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = rc.person_bankrupt_id) AND 
             rc.simple_normalized_cedent = (SELECT TOP 1 normalized_creditor FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = rc.person_bankrupt_id)
            THEN 'Полное совпадение (без наших ключевых слов)'
        WHEN NOT EXISTS (SELECT 1 FROM bankrupt_creditors bc WHERE bc.person_bankrupt_id = rc.person_bankrupt_id)
            THEN 'Кредитор не заявлен'
        ELSE 'Нет наших ключевых слов'
    END AS 'Статус совпадения',
    rc.priority_order
FROM ranked_contragents rc
LEFT JOIN contract_avtos cv ON rc.contract_id = cv.contract_id
WHERE rc.rn = 1 and rc.priority_order <> 6
ORDER BY rc.contract_id,
    rc.priority_order
    ;
