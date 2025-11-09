-- Bulk loading or copying from csv file dataset
-- The method is first truncate and then insert. 
-- Truncate helps when new data has been added to the table, and when we want to insert it newly
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    -- crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info 
    FROM 'D:/Data Science/DWH project/datasets/source_crm/cust_info.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    -- prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info 
    FROM 'D:/Data Science/DWH project/datasets/source_crm/prd_info.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    -- sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM 'D:/Data Science/DWH project/datasets/source_crm/sales_details.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    -- LOC_A101
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM 'D:/Data Science/DWH project/datasets/source_erp/LOC_A101.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    -- CUST_AZ12
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM 'D:/Data Science/DWH project/datasets/source_erp/CUST_AZ12.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    -- PX_CAT_G1V2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM 'D:/Data Science/DWH project/datasets/source_erp/PX_CAT_G1V2.csv' 
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );
END;
$$;
