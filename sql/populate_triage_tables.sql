-- Populating the bill_id_mapping table
INSERT INTO 
    raw.bill_id_mapping (bill_id) 
SELECT DISTINCT bill_id FROM raw.bills;