CREATE OR REPLACE PROCEDURE [REDACTED: removed for NDA/security/privacy](report_date date)
LANGUAGE plpgsql
AS $_$
BEGIN


    DROP TABLE IF EXISTS tmp__inputs;
    CREATE TEMP TABLE tmp__inputs AS
    SELECT
        DATEADD(D, (-DATE_PART(DOW, COALESCE(report_date, CURRENT_DATE))::INTEGER) - 7, COALESCE(report_date, CURRENT_DATE))::DATE AS min_date,
        DATEADD(D, (-DATE_PART(DOW, COALESCE(report_date, CURRENT_DATE))::INTEGER) - 1, COALESCE(report_date, CURRENT_DATE))::DATE AS max_date,
        DATEADD(D, (-DATE_PART(DOW, COALESCE(report_date, CURRENT_DATE))::INTEGER) - 21, COALESCE(report_date, CURRENT_DATE))::DATE AS extended_date;
        

-- Replacing package type names that matches with [REDACTED: removed for NDA/security/privacy]
    DROP TABLE IF EXISTS tmp__sp;
    CREATE TEMP TABLE tmp__sp AS
    SELECT 
        *,
        CASE
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            WHEN package_type = [REDACTED: removed for NDA/security/privacy] THEN [REDACTED: removed for NDA/security/privacy]
            ELSE package_type
        END AS package_type_name
    FROM [REDACTED: removed for NDA/security/privacy]
    WHERE created_at::DATE BETWEEN (SELECT extended_date FROM tmp__inputs) AND (SELECT max_date FROM tmp__inputs);
-- Replacing package type names that matches with [REDACTED: removed for NDA/security/privacy]

    
-- Unpivoting customer rate card tables for look up
    DROP TABLE IF EXISTS tmp__rate_card;
    CREATE TEMP TABLE tmp__rate_card AS
    SELECT 
        [REDACTED: removed for NDA/security/privacy],
        service,
        [REDACTED: removed for NDA/security/privacy],
        rate_measure_min,
        [REDACTED: removed for NDA/security/privacy],
        rate_measure_label,
        REPLACE(zone, 'zone_', '') AS zone,
        [REDACTED: removed for NDA/security/privacy],
        effective_start_date,
        effective_end_date
    FROM (
        SELECT 
            provider_contract_id,
            service,
            type,
            rate_measure_min,
            [REDACTED: removed for NDA/security/privacy],
            rate_measure_label,
            [REDACTED: removed for NDA/security/privacy],
            effective_end_date,
            zone_1,
            [REDACTED: removed for NDA/security/privacy],
            zone_3,
            [REDACTED: removed for NDA/security/privacy],
            zone_5,
            [REDACTED: removed for NDA/security/privacy],
            zone_7,
            [REDACTED: removed for NDA/security/privacy],
            zone_9
        FROM [REDACTED: removed for NDA/security/privacy]
    ) 
    UNPIVOT (rate FOR zone IN ([REDACTED: removed for NDA/security/privacy] zone_7, zone_8, zone_9));
-- Unpivoting rate card tables for look up


-- Unpivoting CP rate card tables for look up
    DROP TABLE IF EXISTS tmp__cp_rate_card;
    CREATE TEMP TABLE tmp__cp_rate_card AS
    SELECT 
        [REDACTED: removed for NDA/security/privacy],
        service,
        [REDACTED: removed for NDA/security/privacy],
        rate_measure_min,
        [REDACTED: removed for NDA/security/privacy],
        rate_measure_label,
        REPLACE(zone, 'zone_', '') AS zone,
        rate,
        [REDACTED: removed for NDA/security/privacy],
        effective_end_date
    FROM (
        SELECT 
            provider_contract_id,
            service,
            type,
            rate_measure_min,
            [REDACTED: removed for NDA/security/privacy],
            rate_measure_label,
            [REDACTED: removed for NDA/security/privacy],
            effective_end_date,
            zone_1,
            [REDACTED: removed for NDA/security/privacy],
            zone_3,
            [REDACTED: removed for NDA/security/privacy],
            zone_5,
            [REDACTED: removed for NDA/security/privacy],
            zone_7,
            [REDACTED: removed for NDA/security/privacy],
            zone_9
        FROM [REDACTED: removed for NDA/security/privacy]
        WHERE [REDACTED: removed for NDA/security/privacy]_id = 1000
    ) 
    UNPIVOT (rate FOR zone IN ([REDACTED: removed for NDA/security/privacy] zone_7, zone_8, zone_9));
-- Unpivoting CP rate card tables for look up


-- Checking customer cubic rate card tables
    DROP TABLE IF EXISTS tmp__cubic_checker;
    CREATE TEMP TABLE tmp__cubic_checker AS
    WITH cte_config_unpivot AS (
        SELECT CAST(key AS INT) AS account_id, key, config
        FROM [REDACTED: removed for NDA/security/privacy] c, UNPIVOT c.value AS config AT key
        WHERE id = 1
    ),
    cubic_list AS (
        SELECT cu.key AS account_id, CAST(config_value AS BOOL) AS account_gets_cubic 
        FROM cte_config_unpivot cu, UNPIVOT cu.config AS config_value AT config_key
        WHERE config_key = 'gets_cubic' AND JSON_SERIALIZE(config_value) = 'true'
    )
    SELECT DISTINCT
        cl.account_id,
        rc.service,
        rc.effective_start_date,
        rc.effective_end_date
    FROM cubic_list cl
    LEFT JOIN tmp__rate_card rc
    ON [REDACTED: removed for NDA/security/privacy]ount_id
    WHERE [REDACTED: removed for NDA/security/privacy];
-- Checking customer cubic rate card tables


-- Extra service list. Note: All extra service fees are included from the sad table. No need to add additionally. 
    DROP TABLE IF EXISTS tmp__service_fee;
    CREATE TEMP TABLE tmp__service_fee AS 
    SELECT
        sad.account_id,
        sad.shipment_id,
        [REDACTED: removed for NDA/security/privacy],
        sad.adjustment_amount * -1 AS adjustment_amount_sf,
        sad.adjustment_note
    FROM [REDACTED: removed for NDA/security/privacy] sad
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] mt
    ON sad.[REDACTED: removed for NDA/security/privacy] = mt.[REDACTED: removed for NDA/security/privacy]
    WHERE [REDACTED: removed for NDA/security/privacy] = '[REDACTED: removed for NDA/security/privacy]'
    AND adjustment_amount <> '0'
    AND [REDACTED: removed for NDA/security/privacy] LIKE '%[REDACTED: removed for NDA/security/privacy]%'
    AND mt.created_at::DATE BETWEEN (SELECT extended_date FROM tmp__inputs) AND (SELECT max_date FROM tmp__inputs);
-- Extra service list. Note: All extra service fees are included from the sad table. No need to add additionally. 


-- Grabbing total sum of adjustment_type after filterting. This is done in case there are multiple records per shipment_id (for pdx shipments)
    DROP TABLE IF EXISTS filtered_sad;
    CREATE TEMP TABLE filtered_sad AS 
    WITH temp AS (
        SELECT
            shipment_id,
            ROUND(SUM(ROUND([REDACTED: removed for NDA/security/privacy], 2)), 2) AS claimed_amount
        FROM [REDACTED: removed for NDA/security/privacy]
        WHERE adjustment_type = '[REDACTED: removed for NDA/security/privacy]'
        AND adjustment_source = '[REDACTED: removed for NDA/security/privacy]'
        GROUP BY shipment_id
    ),
    temp2 AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY created_at DESC) AS rank
        FROM [REDACTED: removed for NDA/security/privacy]
        WHERE adjustment_type = '[REDACTED: removed for NDA/security/privacy]'
        AND adjustment_source = '[REDACTED: removed for NDA/security/privacy]'
    ),
    temp3 AS (
        
        SELECT
            CAST([REDACTED: removed for NDA/security/privacy] AS DECIMAL(10,2)) AS claimed_amount,
            t2.*
        FROM temp2 t2
        LEFT JOIN temp t ON t2.shipment_id = t.shipment_id
        WHERE t2.rank = 1
    ),
    temp4 AS (
        SELECT
            s.carrier,
            mt.*
        FROM [REDACTED: removed for NDA/security/privacy] mt
        LEFT JOIN [REDACTED: removed for NDA/security/privacy] s
        ON mt.shipment_id = s.id
        WHERE mt.[REDACTED: removed for NDA/security/privacy]::DATE BETWEEN (SELECT extended_date FROM tmp__inputs) AND (SELECT max_date FROM tmp__inputs)
        AND mt.transaction_type <> '[REDACTED: removed for NDA/security/privacy]'
        AND mt.[REDACTED: removed for NDA/security/privacy] = '1'
        AND mt.deleted_at IS NULL
        AND s.carrier = '[REDACTED: removed for NDA/security/privacy]'
    )
    SELECT
        t3.*
    FROM temp3 t3
    INNER JOIN temp4 t4
    ON t4.shipment_id = t3.shipment_id;
-- Grabbing total sum of adjustment_type after filterting. This is done in case there are multiple records per shipment_id (for pdx shipments)


-- Joining and filterting main table before transformation
    DROP TABLE IF EXISTS tmp__sad0;
    CREATE TEMP TABLE tmp__sad0 AS
    SELECT 
        m.name AS distributor,
        a.name AS account_reference,
        sad.*,
        s.account_id AS s_account_id, -- [REDACTED: removed for NDA/security/privacy]
        sp.package_type_name,
        -- Can't use this because it's affecting customer's weight as well. Use this for our cost validation
        -- [REDACTED: removed for NDA/security/privacy]
        [REDACTED: removed for NDA/security/privacy],
        sp.length,
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        CASE WHEN cc.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS cubic_flag,
        s.carrier,
        [REDACTED: removed for NDA/security/privacy],
        se.zone,
        [REDACTED: removed for NDA/security/privacy],
        se.from_country,
        se.to_zip,
        s.created_at AS label_created_at,
        CASE
            WHEN urzc.zip_code_prefix IS NOT NULL THEN 1
            WHEN uezc.zip_code IS NOT NULL AND (ae.[REDACTED: removed for NDA/security/privacy] = TRUE) AND (i.[REDACTED: removed for NDA/security/privacy] = '[REDACTED: removed for NDA/security/privacy]') THEN 2 -- [REDACTED: removed for NDA/security/privacy]
            WHEN uezc.zip_code IS NOT NULL AND (i.[REDACTED: removed for NDA/security/privacy] = '[REDACTED: removed for NDA/security/privacy]') THEN 3
            ELSE 0
        END AS r_e_zip_flag,
        CASE
            WHEN r_e_zip_flag = 1 THEN 'Package is sent to [REDACTED: removed for NDA/security/privacy]. '
            WHEN r_e_zip_flag = 2 THEN 'Package is sent to [REDACTED: removed for NDA/security/privacy]. '
            WHEN r_e_zip_flag = 3 THEN 'Package is sent to [REDACTED: removed for NDA/security/privacy]. '
            ELSE ''
        END AS r_e_zip_note,
        se.[REDACTED: removed for NDA/security/privacy],
        s.customer_reference_1,
        s.[REDACTED: removed for NDA/security/privacy],
        pa.name as [REDACTED: removed for NDA/security/privacy],
        CASE 
            -- If dimensional weight applies (volume > [REDACTED: removed for NDA/security/privacy])
            WHEN ROUND(sp.length) * ROUND(sp.width) * ROUND(sp.height) > [REDACTED: removed for NDA/security/privacy] THEN 
                CASE
                    -- Girth check for GA packages. Rouding every dimension since it doesnt specify. Rounds for [REDACTED: removed for NDA/security/privacy]
                    WHEN s.service = 'ground_advantage'
                        -- SUM OF ALL - MAX --> girth
                        -- IF MAX + ((SUM OF ALL - MAX) * 2) BETWEEN 108 and 130, consider the package as OVERSIZED
                        AND (GREATEST(ROUND(sp.length), ROUND(sp.width), ROUND(sp.height) + (((ROUND(sp.length) + ROUND(sp.width) + ROUND(sp.height)) - GREATEST(ROUND(sp.length), ROUND(sp.width), ROUND(sp.height))) * 2)))
                        BETWEEN 108 AND 130 THEN 75
                    WHEN GREATEST(
                        CEIL(ROUND(sp.length) * ROUND(sp.width) * ROUND(sp.height) / 166), 
                        weight_oz / 16.0
                    ) > 70 THEN 75
                    ELSE GREATEST(
                        CEIL(ROUND(sp.length) * ROUND(sp.width) * ROUND(sp.height) / 166), 
                        weight_oz / 16.0
                    )
                END
            ELSE weight_oz / 16.0
        END AS [REDACTED: removed for NDA/security/privacy],
        CASE
            WHEN (ROUND(sp.length) * ROUND(sp.width) * ROUND(sp.height) > 1728) AND (CEIL(ROUND(sp.length) * ROUND(sp.width) * ROUND(sp.height) / 166) > weight_oz / 16.0) THEN '[REDACTED: removed for NDA/security/privacy]'
            ELSE '[REDACTED: removed for NDA/security/privacy]'
        END AS [REDACTED: removed for NDA/security/privacy],
        -- Pre-calculating adjusted dimensions for cubic detection
        CASE WHEN sp.length < 0.25 THEN sp.length ELSE FLOOR(sp.length / 0.25) * 0.25 END AS adj_length,
        CASE WHEN sp.width < 0.25 THEN sp.width ELSE FLOOR(sp.width / 0.25) * 0.25 END AS adj_width,
        CASE WHEN sp.height < 0.25 THEN sp.height ELSE FLOOR(sp.height / 0.25) * 0.25 END AS adj_height,
        CASE
            WHEN bill_weight_lb = [REDACTED: removed for NDA/security/privacy] THEN 'Package is oversized. '
            WHEN bill_weight_lb <> (weight_oz/ 16) AND package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN '[REDACTED: removed for NDA/security/privacy] its actual weight. [REDACTED: removed for NDA/security/privacy]. Dimmensional weight: ' || ROUND(bill_weight_lb, 2) || ' lb <=> ' || ROUND(bill_weight_lb, 2) * 16.00 || ' oz. '
            WHEN bill_weight_lb = (weight_oz/ 16) AND package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN 'Weight: ' || ROUND(bill_weight_lb, 2) || ' lb <=> ' || ROUND(weight_oz, 2) || ' oz. '
        END AS [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy] AS cp_id,
        CASE
            WHEN pa.name ILIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN 1
            WHEN pa.name NOT ILIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN 0
            ELSE NULL
        END AS [REDACTED: removed for NDA/security/privacy],
        CASE
            WHEN r_e_zip_flag = 1 OR r_e_zip_flag = 2 THEN 1 -- CP rate card
            WHEN r_e_zip_flag = 3 THEN 2 -- Exemption rate card
            ELSE 0 -- normal customer rate card
        END AS [REDACTED: removed for NDA/security/privacy],
        CASE 
            WHEN package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND [REDACTED: removed for NDA/security/privacy] = TRUE AND s.[REDACTED: removed for NDA/security/privacy] = '[REDACTED: removed for NDA/security/privacy]' 
                AND (adj_length <= 18 AND adj_width <= 18 AND adj_height <= 18) 
                AND ([REDACTED: removed for NDA/security/privacy] <= 20) 
                AND (adj_length * adj_width * adj_height) / 1728 <= 1 
            THEN (adj_length * adj_width * adj_height) / [REDACTED: removed for NDA/security/privacy]
            WHEN package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND [REDACTED: removed for NDA/security/privacy] = TRUE AND s.[REDACTED: removed for NDA/security/privacy] = '[REDACTED: removed for NDA/security/privacy]' 
                AND (adj_length <= 18 AND adj_width <= 18 AND adj_height <= 18) 
                AND ([REDACTED: removed for NDA/security/privacy] <= 20) 
                AND (adj_length * adj_width * adj_height) / 1728 <= 0.5 
            THEN (adj_length * adj_width * adj_height) / [REDACTED: removed for NDA/security/privacy]
            ELSE NULL
        END AS [REDACTED: removed for NDA/security/privacy]
    FROM [REDACTED: removed for NDA/security/privacy] sad
    LEFT JOIN tmp__sp sp
        ON sad.shipment_id = sp.shipment_id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] m
        ON sad.meter_id = m.id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] a
        ON sad.account_id = a.id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] se
        ON se.shipment_id = sad.shipment_id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] s
        ON sad.shipment_id = s.id
    LEFT JOIN tmp__cubic_checker cc
        ON sad.account_id = cc.account_id
        AND cc.service LIKE '%' || s.[REDACTED: removed for NDA/security/privacy] || '%'
        AND s.created_at::DATE BETWEEN cc.effective_start_date AND cc.effective_end_date
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] ae
        ON sad.account_id = ae.account_id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] pa
        ON pa.id = s.provider_account_id
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] urzc
        ON se.to_zip LIKE urzc.zip_code_compare_expression
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] uezc
        ON se.to_zip LIKE uezc.zip_code_compare_expression
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] i
        ON sad.shipment_id = i.shipment_id
    WHERE s.created_at::DATE
        BETWEEN (SELECT min_date FROM tmp__inputs) 
        AND (SELECT max_date FROM tmp__inputs)
        AND s.carrier = 'usps';
-- Joining and filterting main table before transformation


-- Look up logic
    DROP TABLE IF EXISTS tmp__sad;
    WITH t1 AS (
        SELECT 
            ss.*,
            '[REDACTED: removed for NDA/security/privacy]: ' || ss.service || '. ' AS service_note,
            (ss.adj_length * ss.adj_width * ss.adj_height) / [REDACTED: removed for NDA/security/privacy] AS cubic_size_raw,
            CASE
                WHEN cubic_size IS NOT NULL THEN 'Cubic checker shows the customer is eligible for cubic tier: ' || ROUND((CEIL(cubic_size * 10) / 10), 2) || '. '
                WHEN cubic_size IS NULL AND ss.cubic_flag = TRUE AND ss.service = 'ground_advantage' THEN 'Cubic checker shows that the customer is eligible for cubic rate but the package is greater than 1 cubic therefore not eligible for cubic rate. '
                WHEN cubic_size IS NULL AND ss.cubic_flag = TRUE AND ss.service = 'priority'  THEN 'Cubic checker shows that the customer is eligible for cubic rate but the package is greater than 0.5 cubic therefore not eligible for cubic rate. '
                WHEN ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND cubic_size IS NULL THEN 'Cubic checker did not find cubic eligibilty for this customer. '
            END AS cubic_note,
            CASE
                WHEN ss.service = 'priority' AND (ss.length > 30 OR ss.width > 30 OR ss.height > 30) THEN [REDACTED: removed for NDA/security/privacy]
                WHEN ss.service = 'ground_advantage' AND (ss.length > 30 OR ss.width > 30 OR ss.height > 30) THEN [REDACTED: removed for NDA/security/privacy]
                WHEN ss.service IN ('ground_advantage', 'priority') AND (ss.length > 22 OR ss.width > 22 OR ss.height > 22) THEN [REDACTED: removed for NDA/security/privacy]
                ELSE 0
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN dimension_fee = [REDACTED: removed for NDA/security/privacy] THEN 'One of the dimensions is greater [REDACTED: removed for NDA/security/privacy] '
                WHEN dimension_fee = [REDACTED: removed for NDA/security/privacy] THEN 'One of the dimensions is greater [REDACTED: removed for NDA/security/privacy] '
                WHEN dimension_fee = [REDACTED: removed for NDA/security/privacy] THEN 'One of the dimensions is greater [REDACTED: removed for NDA/security/privacy]' 
                ELSE NULL
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN ss.service = '[REDACTED: removed for NDA/security/privacy]' 
                    AND ((ROUND(ss.length) * ROUND(ss.width) * ROUND(ss.height)) / 1728) > 2 THEN [REDACTED: removed for NDA/security/privacy]
                WHEN ss.service = '[REDACTED: removed for NDA/security/privacy]' 
                    AND ((ROUND(ss.length) * ROUND(ss.width) * ROUND(ss.height)) / 1728) > 2 THEN [REDACTED: removed for NDA/security/privacy]
                ELSE 0
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN cubic_fee = [REDACTED: removed for NDA/security/privacy] THEN 'Package exceeds [REDACTED: removed for NDA/security/privacy] '
                WHEN cubic_fee = [REDACTED: removed for NDA/security/privacy] THEN 'Package exceeds [REDACTED: removed for NDA/security/privacy] '
                ELSE NULL
            END AS cubic_fee_note,
            '[REDACTED: removed for NDA/security/privacy]' || ss.package_type_name || '. ' AS package_type_note,
            CASE
                WHEN ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' THEN ''
                WHEN (ss.rc_type = 0 OR ss.rc_type = 2) AND CAST(rcc.rate AS DECIMAL(10,2)) IS NOT NULL THEN '[REDACTED: removed for NDA/security/privacy]'
                WHEN ss.rc_type = 1 AND CAST(cprcc.rate AS DECIMAL(10,2)) IS NOT NULL THEN '[REDACTED: removed for NDA/security/privacy]'
                WHEN (ss.rc_type = 0 OR ss.rc_type = 2) AND CAST(rcc.rate AS DECIMAL(10,2)) IS NULL THEN '[REDACTED: removed for NDA/security/privacy]'
                WHEN ss.rc_type = 1 AND CAST(cprcc.rate AS DECIMAL(10,2)) IS NULL THEN '[REDACTED: removed for NDA/security/privacy]'
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN cubic_note NOT LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND ss.rc_type = 1 THEN CAST(cprcc.rate AS DECIMAL(10,2))
                WHEN cubic_not_found_note = '[REDACTED: removed for NDA/security/privacy]' THEN CAST(9999 AS DECIMAL(10,2)) 
                WHEN cubic_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN CAST(9999 AS DECIMAL(10,2))
                WHEN cubic_not_found_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN CAST(9999 AS DECIMAL(10,2))
                WHEN ss.rc_type = 1 THEN CAST(cprcc.rate AS DECIMAL(10,2))
                WHEN (ss.rc_type = 0 OR ss.rc_type = 2) THEN CAST(rcc.rate AS DECIMAL(10,2))
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN cubic_not_found_note = '[REDACTED: removed for NDA/security/privacy]' THEN CAST(9999 AS DECIMAL(10,2)) 
                WHEN cubic_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN CAST(9999 AS DECIMAL(10,2))
                WHEN cubic_not_found_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN CAST(9999 AS DECIMAL(10,2))
                ELSE CAST(cprcc.rate AS DECIMAL(10,2))
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN ss.service IN ('[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]') THEN '[REDACTED: removed for NDA/security/privacy]'

                WHEN (
                    ((ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND CAST(rcb.rate AS DECIMAL(10,2)) IS NULL
                    )
                    AND
                    (ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND CAST(cprcb.rate AS DECIMAL(10,2)) IS NOT NULL
                    )
                    AND ss.bill_weight_lb > [REDACTED: removed for NDA/security/privacy])
                ) THEN 'Package dimension exceeds [REDACTED: removed for NDA/security/privacy]. '

                WHEN (
                    (ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND ss.package_type_name IN (
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', 
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]'
                        )
                        AND CAST(rcf.rate AS DECIMAL(10,2)) IS NULL 
                    ) 
                    OR 
                    (ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND CAST(rcb.rate AS DECIMAL(10,2)) IS NULL
                    )
                ) THEN 'Customer base rate card was not found. Check with IT and Sales. '

                WHEN (
                    (ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type = 1 
                        AND ss.package_type_name IN (
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', 
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]'
                        )
                        AND CAST(rcf.rate AS DECIMAL(10,2)) IS NULL 
                    ) 
                    OR 
                    (ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type = 1 
                        AND CAST(cprcb.rate AS DECIMAL(10,2)) IS NULL
                    )
                ) THEN '[REDACTED: removed for NDA/security/privacy]. '

                WHEN (
                    (ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND ss.package_type_name IN (
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', 
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]'
                        )
                        AND CAST(rcf.rate AS DECIMAL(10,2)) IS NOT NULL 
                    ) 
                    OR 
                    (ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type IN (0, 2) 
                        AND CAST(rcb.rate AS DECIMAL(10,2)) IS NOT NULL
                    )
                ) THEN 'Customer base rate card was found. '

                WHEN (
                    (ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type = 1 
                        AND ss.package_type_name IN (
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', 
                            '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]'
                        )
                        AND CAST(rcf.rate AS DECIMAL(10,2)) IS NOT NULL 
                    ) 
                    OR 
                    (ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' 
                        AND ss.rc_type = 1 
                        AND CAST(cprcb.rate AS DECIMAL(10,2)) IS NOT NULL
                    )
                ) THEN '[REDACTED: removed for NDA/security/privacy]' 

                ELSE 'SOMETHING WRONG W BASE_NOT_FOUND_NOTE. '
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN ss.service IN ('[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', '[REDACTED: removed for NDA/security/privacy]', ) THEN ROUND(ss.adjustment_amount, 2)::DECIMAL(10,2)
                WHEN base_not_found_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND (ss.rc_type = 0 OR ss.rc_type = 2) AND ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN CAST(cprcb.rate AS DECIMAL(10,2))
                WHEN base_not_found_note LIKE '%[REDACTED: removed for NDA/security/privacy]%' THEN CAST(9999 AS DECIMAL(10,2))
                WHEN base_not_found_note NOT LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND ss.rc_type = 1 AND ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' THEN CAST(cprcf.rate AS DECIMAL(10,2))
                WHEN base_not_found_note NOT LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND ss.rc_type = 1 AND ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN CAST(cprcb.rate AS DECIMAL(10,2))
                WHEN base_not_found_note NOT LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND (ss.rc_type = 0 OR ss.rc_type = 2) AND ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' THEN CAST(rcf.rate AS DECIMAL(10,2))
                WHEN base_not_found_note NOT LIKE '%[REDACTED: removed for NDA/security/privacy]%' AND (ss.rc_type = 0 OR ss.rc_type = 2) AND ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN CAST(rcb.rate AS DECIMAL(10,2))
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND (cubic_price <= base_price) THEN '[REDACTED: removed for NDA/security/privacy]'
                WHEN ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND (cubic_price >= base_price) THEN '[REDACTED: removed for NDA/security/privacy]'
                WHEN ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' THEN '[REDACTED: removed for NDA/security/privacy]'
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN cheaper_price = '[REDACTED: removed for NDA/security/privacy]' THEN 'Base rate: $' || base_price || '. Cubic rate: $' || cubic_price || '. [REDACTED: removed for NDA/security/privacy]. '
                WHEN cheaper_price = '[REDACTED: removed for NDA/security/privacy]' AND cubic_price <> 9999 THEN 'Base rate: $' || base_price || '. Cubic rate: $' || cubic_price || '. [REDACTED: removed for NDA/security/privacy]. '
                WHEN cheaper_price = '[REDACTED: removed for NDA/security/privacy]' AND cubic_price = 9999 THEN 'Base rate: $' || base_price || '. '
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE 
                WHEN sf.[REDACTED: removed for NDA/security/privacy] IS NOT NULL THEN '[REDACTED: removed for NDA/security/privacy]' || sf.adjustment_note || ' was applied for $' || CAST(sf.[REDACTED: removed for NDA/security/privacy] AS DECIMAL(10,2)) || '.'
                ELSE NULL
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE
                WHEN ss.package_type_name <> '[REDACTED: removed for NDA/security/privacy]' THEN base_price
                WHEN ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' AND (([REDACTED: removed for NDA/security/privacy] + [REDACTED: removed for NDA/security/privacy] + cubic_fee) = [REDACTED: removed for NDA/security/privacy]) THEN base_price + di[REDACTED: removed for NDA/security/privacy]mension_fee + cubic_fee
                WHEN ss.package_type_name = '[REDACTED: removed for NDA/security/privacy]' THEN (LEAST(base_price, [REDACTED: removed for NDA/security/privacy] ) + dimension_fee + [REDACTED: removed for NDA/security/privacy])
                ELSE 0
            END AS [REDACTED: removed for NDA/security/privacy],
            CASE WHEN CAST(billed_amount_temp AS DECIMAL(10,2)) < CAST(sad.[REDACTED: removed for NDA/security/privacy] * -1 AS DECIMAL(10,2)) THEN CAST(sad.adjustment_amount * -1 AS DECIMAL(10,2)) ELSE billed_amount_temp END AS billed_amount,
            CASE WHEN CAST(billed_amount_temp AS DECIMAL(10,2)) < CAST(sad.[REDACTED: removed for NDA/security/privacy] * -1 AS DECIMAL(10,2)) THEN 'USPS overcharged this package. Auto-Dispute is submitted to USPS. ' ELSE NULL END AS auto_dispute_note,
            'Total: $' || CAST([REDACTED: removed for NDA/security/privacy] AS DECIMAL(10,2)) || '. ' AS [REDACTED: removed for NDA/security/privacy],
            CASE WHEN (billed_amount IS NULL OR billed_amount = 9999) THEN 'TRUE' END AS empty_price,
            ss.claimed_amount - billed_amount AS billed_adjustment_amount,
            CASE -- Flag for big difference from claimed_amount vs rate card price.
                WHEN ABS(ROUND(billed_adjustment_amount, 2)) > 15 THEN '[REDACTED: removed for NDA/security/privacy]. ' ELSE NULL END AS [REDACTED: removed for NDA/security/privacy],
            CASE WHEN gs.shipment_id IS NOT NULL THEN '[REDACTED: removed for NDA/security/privacy].' ELSE NULL END AS [REDACTED: removed for NDA/security/privacy]
        FROM [REDACTED: removed for NDA/security/privacy] ss
        LEFT JOIN tmp__rate_card rcb
            ON ss.[REDACTED: removed for NDA/security/privacy] = rcb.[REDACTED: removed for NDA/security/privacy]
            AND rcb.type = 'basic'
            AND ss.[REDACTED: removed for NDA/security/privacy] = rcb.[REDACTED: removed for NDA/security/privacy]
            AND ss.bill_weight_lb BETWEEN rcb.rate_measure_min AND rcb.rate_measure_max
            AND rcb.service LIKE '%' || ss.service || '%'
            AND ss.label_created_at::DATE BETWEEN rcb.effective_start_date AND rcb.effective_end_date
        LEFT JOIN tmp__rate_card rcc
            ON ss.s_account_id = rcc.account_id
            AND rcc.type = 'cubic'
            AND ss.[REDACTED: removed for NDA/security/privacy] = rcc.[REDACTED: removed for NDA/security/privacy]
            AND ss.cubic_size BETWEEN rcc.rate_measure_min AND rcc.rate_measure_max
            AND rcc.service LIKE '%' || ss.service || '%'
            AND ss.label_created_at::DATE BETWEEN rcc.effective_start_date AND rcc.effective_end_date
        LEFT JOIN tmp__rate_card rcf
            ON ss.s_account_id = rcf.account_id
            AND rcf.[REDACTED: removed for NDA/security/privacy] = '1' -- Fetching the first price using zone 1, as all zones have the same pricing
            AND ss.package_type_name = rcf.rate_measure_label
            AND ss.label_created_at::DATE BETWEEN rcf.effective_start_date AND rcf.effective_end_date
        LEFT JOIN tmp__cp_rate_card cprcb
            ON ss.cp_id = cprcb.provider_contract_id
            AND cprcb.type = 'basic'
            AND ss.[REDACTED: removed for NDA/security/privacy] = cprcb.[REDACTED: removed for NDA/security/privacy]
            AND ss.bill_weight_lb BETWEEN cprcb.rate_measure_min AND cprcb.rate_measure_max
            AND cprcb.service LIKE '%' || ss.service || '%'
            AND ss.label_created_at::DATE BETWEEN cprcb.effective_start_date AND cprcb.effective_end_date
        LEFT JOIN tmp__cp_rate_card cprcc
            ON ss.cp_id = cprcc.provider_contract_id
            AND cprcc.type = 'cubic'
            AND ss.[REDACTED: removed for NDA/security/privacy] = cprcc.[REDACTED: removed for NDA/security/privacy]
            AND ss.cubic_size BETWEEN cprcc.rate_measure_min AND cprcc.rate_measure_max
            AND cprcc.service LIKE '%' || ss.service || '%'
            AND ss.label_created_at::DATE BETWEEN cprcc.effective_start_date AND cprcc.effective_end_date
        LEFT JOIN tmp__cp_rate_card cprcf
            ON ss.cp_id = cprcf.provider_contract_id
            AND cprcf.[REDACTED: removed for NDA/security/privacy] = '1'  -- Fetching the first price using zone 1, as all zones have the same pricing
            AND ss.package_type_name = cprcf.rate_measure_label
            AND ss.label_created_at::DATE BETWEEN cprcf.effective_start_date AND cprcf.effective_end_date
        LEFT JOIN tmp__service_fee sf
            ON ss.[REDACTED: removed for NDA/security/privacy] = sf.[REDACTED: removed for NDA/security/privacy]
        LEFT JOIN [REDACTED: removed for NDA/security/privacy] gs
            ON ss.[REDACTED: removed for NDA/security/privacy] = gs.[REDACTED: removed for NDA/security/privacy]
        LEFT JOIN [REDACTED: removed for NDA/security/privacy] sad 
            ON ss.[REDACTED: removed for NDA/security/privacy] = sad.[REDACTED: removed for NDA/security/privacy] 
            AND sad.adjustment_type = 'shipped_postage'
    )
    SELECT * INTO TEMP tmp__sad FROM t1;
-- Look up logic


-- Null checker
    DROP TABLE IF EXISTS sad_null_update;
    CREATE TABLE sad_null_update AS
    SELECT
        *,
        TRIM(BOTH ', ' FROM 
            CASE WHEN distributor IS NULL THEN 'distributor is null, ' ELSE NULL END ||
            CASE WHEN shipment_id IS NULL THEN 'shipment_id is null, ' ELSE NULL END ||
            CASE WHEN account_id IS NULL THEN 'account_id is null, ' ELSE NULL END ||
            CASE WHEN label_created_at IS NULL THEN 'label_created_at is null, ' ELSE NULL END ||
            CASE WHEN carrier IS NULL THEN 'carrier is null, ' ELSE NULL END ||
            CASE WHEN service IS NULL THEN 'service is null, ' ELSE NULL END ||
            CASE WHEN cheaper_price IS NULL THEN 'rate_type is null, ' ELSE NULL END ||
            CASE WHEN tracking_number IS NULL THEN 'tracking_number is null, ' ELSE NULL END ||
            CASE WHEN from_zip IS NULL THEN 'from_zip is null, ' ELSE NULL END ||
            CASE WHEN from_country IS NULL THEN 'from_country is null, ' ELSE NULL END ||
            CASE WHEN to_zip IS NULL THEN 'to_zip is null, ' ELSE NULL END ||
            CASE WHEN to_country IS NULL THEN 'to_country is null, ' ELSE NULL END ||
            CASE WHEN package_type_name IS NULL THEN 'package_type_name is null, ' ELSE NULL END ||
            CASE WHEN weight_oz IS NULL THEN 'weight_oz is null, ' ELSE NULL END ||
            CASE WHEN bill_weight_lb IS NULL THEN 'bill_weight_lb is null, ' ELSE NULL END ||
            CASE WHEN billed_using_dim_weight_flag IS NULL THEN 'billed_using_dim_weight_flag is null, ' ELSE NULL END ||
            CASE WHEN zone IS NULL THEN 'zone is null, ' ELSE NULL END ||
            CASE WHEN length IS NULL THEN 'length is null, ' ELSE NULL END ||
            CASE WHEN width IS NULL THEN 'width is null, ' ELSE NULL END ||
            CASE WHEN height IS NULL THEN 'height is null, ' ELSE NULL END ||
            CASE WHEN billed_amount IS NULL THEN 'billed_amount is null, ' ELSE NULL END ||
            CASE WHEN claimed_amount IS NULL THEN 'claimed_amount is null, ' ELSE NULL END ||
            CASE WHEN billed_adjustment_amount IS NULL THEN 'billed_adjustment_amount is null, ' ELSE NULL END
        ) AS null_note
    FROM tmp__sad;
-- Null checker


-- Finalizing; selecting required columns
    DROP TABLE IF EXISTS final_temp;
    CREATE TABLE final_temp AS
    SELECT
        COALESCE(auto_dispute_note, '') ||
        COALESCE(service_note, '') || 
        COALESCE(package_type_note, '') || 
        COALESCE(weight_note, '') ||
        COALESCE(r_e_zip_note, '') ||
        COALESCE(base_not_found_note, '') ||
        COALESCE(cubic_note, '') || 
        COALESCE(cubic_not_found_note, '') ||
        COALESCE(price_note, '') ||
        COALESCE(dimension_fee_note, '') || 
        COALESCE(cubic_fee_note, '') ||
        COALESCE(billed_amount_note, '') ||
        COALESCE(service_fee_note, '') ||
        COALESCE(null_note, '') ||
        COALESCE(got_note, '') ||
        COALESCE(billed_adjustment_amount_note, '') AS calculation_note,
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        s_account_id AS account_id,
        'shipped' AS status,
        label_created_at,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        cheaper_price AS rate_type,
        CAST(tracking_number AS VARCHAR),
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        weight_oz,
        ROUND(bill_weight_lb, 2)::DECIMAL(10,2) AS bill_weight_lb,
        [REDACTED: removed for NDA/security/privacy],
        [REDACTED: removed for NDA/security/privacy],
        length,
        width,
        [REDACTED: removed for NDA/security/privacy],
        billed_amount,
        ROUND([REDACTED: removed for NDA/security/privacy], 2)::DECIMAL(10,2) AS [REDACTED: removed for NDA/security/privacy],
        ROUND([REDACTED: removed for NDA/security/privacy], 2)::DECIMAL(10,2) AS [REDACTED: removed for NDA/security/privacy],
        CASE
            -- When the report range consists of two different months, split it into two reports.
            -- For example, 03.29.2025 - 04.04.2025 --> 03.29.2025 - 03.31.2025 & 04.01.2025 - 04.04.2025
            WHEN EXTRACT(MONTH FROM i.min_date::DATE) != EXTRACT(MONTH FROM i.max_date::DATE)
            THEN
                -- First part of the report (min_date to the end of the month)
                CASE
                    WHEN label_created_at::DATE BETWEEN i.min_date::DATE AND LAST_DAY(i.min_date::DATE)
                    THEN
                        distributor ||
                        ' - [REDACTED: removed for NDA/security/privacy] - ' || 
                        TO_CHAR(i.min_date::DATE, 'YYYY.MM.DD') ||
                        ' - ' || 
                        TO_CHAR(LAST_DAY(i.min_date::DATE), 'YYYY.MM.DD') ||
                        '.csv'
                    -- Second part of the report (start of the next month to max_date)
                    WHEN label_created_at::DATE BETWEEN DATEADD(d, -(DATEPART(D, i.max_date::DATE)-1), i.max_date::DATE)::DATE AND i.max_date::DATE
                    THEN
                        distributor ||
                        ' - [REDACTED: removed for NDA/security/privacy] - ' ||
                        TO_CHAR(DATEADD(d, -(DATEPART(D, i.max_date::DATE)-1), i.max_date::DATE)::DATE, 'YYYY.MM.DD') ||
                        ' - ' ||
                        TO_CHAR(i.max_date::DATE, 'YYYY.MM.DD') ||
                        '.csv'
                END
            -- For ranges within the same month
            ELSE
                distributor || ' - [REDACTED: removed for NDA/security/privacy] - ' || 
                TO_CHAR(i.min_date::DATE, 'YYYY.MM.DD') || ' - ' || 
                TO_CHAR(i.max_date::DATE, 'YYYY.MM.DD') || '.csv'
        END AS report_name,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS_v5.04.08') AS etl_execution_id,
        'CALL [REDACTED: removed for NDA/security/privacy](' || COALESCE('''' || report_date || '''', 'NULL') || ');' AS etl_job_name
    FROM
        sad_null_update
    JOIN tmp__inputs i ON 1=1;

    INSERT INTO [REDACTED: removed for NDA/security/privacy]
    SELECT *
    FROM final_temp ft
    WHERE ft.shipment_id NOT IN (
        SELECT shipment_id
        FROM [REDACTED: removed for NDA/security/privacy]
    )
    AND ABS(ft.billed_adjustment_amount) <= 1000;

    INSERT INTO [REDACTED: removed for NDA/security/privacy]
    SELECT * 
    FROM final_temp ft
    WHERE ABS(billed_adjustment_amount) > 1000 
    OR calculation_note ILIKE '%[REDACTED: removed for NDA/security/privacy]%'
    OR calculation_note LIKE '%[REDACTED: removed for NDA/security/privacy]%';

    -- INSERT INTO [REDACTED: removed for NDA/security/privacy]
    INSERT INTO [REDACTED: removed for NDA/security/privacy]
    SELECT
        ft.account_id,
        (SELECT min_date FROM tmp__inputs) AS _period_start,
        ft.[REDACTED: removed for NDA/security/privacy],
        ft.report_name,
        ft.[REDACTED: removed for NDA/security/privacy] AS impb,
        NULL AS efn,
        i.earliest_scan_date AS first_scan_date,
        i.manifested_datetime AS manifest_date,
        sad.[REDACTED: removed for NDA/security/privacy] AS [REDACTED: removed for NDA/security/privacy],
        ft.etl_job_name,
        ft.etl_execution_id,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at,
        i.[REDACTED: removed for NDA/security/privacy] AS [REDACTED: removed for NDA/security/privacy]
    FROM final_temp ft
    LEFT JOIN [REDACTED: removed for NDA/security/privacy] i
        ON ft.shipment_id = i.shipment_id
    LEFT JOIN tmp__sad sad
        ON ft.shipment_id = sad.shipment_id
    -- Temporary; removing the shipments that were charged as CP from the recent incident
    LEFT JOIN (
        SELECT shipment_id, usps_postage_charged, expected_postage_from_ship_api, file_date
        FROM [REDACTED: removed for NDA/security/privacy]
        WHERE usps_price_type = '[REDACTED: removed for NDA/security/privacy]'
        AND zone != 9
        AND NOT [REDACTED: removed for NDA/security/privacy]
        AND NOT [REDACTED: removed for NDA/security/privacy]
        AND weight <= 20
        AND purchase_contract = '[REDACTED: removed for NDA/security/privacy]'
        AND file_date BETWEEN '2025-04-01' AND '2025-04-07'
    ) a
        ON ft.shipment_id = a.shipment_id
    WHERE ft.calculation_note LIKE '%Auto-Dispute%'
    OR ft.shipment_id <> a.shipment_id;
-- Finalizing; selecting required columns


-- Unloading into S3
    UNLOAD ('
        SELECT
            distributor,
            ''a'' AS account_id,
            report_name,
            [REDACTED: removed for NDA/security/privacy],
            label_created_at,
            carrier,
            [REDACTED: removed for NDA/security/privacy],
            rate_type,
            [REDACTED: removed for NDA/security/privacy],
            from_zip,
            from_country,
            [REDACTED: removed for NDA/security/privacy],
            [REDACTED: removed for NDA/security/privacy],
            [REDACTED: removed for NDA/security/privacy],
            weight_oz,
            bill_weight_lb,
            [REDACTED: removed for NDA/security/privacy],
            zone,
            [REDACTED: removed for NDA/security/privacy],
            width,
            [REDACTED: removed for NDA/security/privacy],
            [REDACTED: removed for NDA/security/privacy],
            claimed_amount,
            [REDACTED: removed for NDA/security/privacy]
        FROM [REDACTED: removed for NDA/security/privacy]
    ')
    TO 's3://[REDACTED: removed for NDA/security/privacy]/'
    IAM_ROLE 'arn:aws:iam::[REDACTED: removed for NDA/security/privacy]38'
    ADDQUOTES DELIMITER ','
    PARTITION BY ([REDACTED: removed for NDA/security/privacy], [REDACTED: removed for NDA/security/privacy])
    PARALLEL OFF HEADER ALLOWOVERWRITE EXTENSION 'csv';
-- Unloading into S3
END
$_$;