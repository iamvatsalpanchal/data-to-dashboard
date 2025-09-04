WITH dim_contacts AS (
    SELECT
        contactid,
        accountid AS contact_account_id,
        firstname AS contact_first_name,
        lastname AS contact_last_name,
        email AS contact_email,
        phone AS contact_phone,
        title AS contact_title,
        city AS contact_city,
        CAST(lastmodifieddate AS DATE) AS contact_last_modified_at
    FROM 
        {{ ref('contacts') }}
)

SELECT
    *
FROM 
    dim_contacts