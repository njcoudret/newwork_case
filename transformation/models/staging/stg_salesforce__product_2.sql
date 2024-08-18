

with source as (

    select * from {{ source('salesforce', 'product_2') }}

),

renamed as (

    select
        id as product_id,
        name,
        productcode as product_code,
        substr(productcode,1,2) as product_category,
        rpad(substr(productcode,1,3),6,'0') as product_series,
        description,
        {{bool('isactive')}} as is_active,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        family,
        externaldatasourceid as external_datasource_id,
        externalid as external_id,
        displayurl,
        quantityunitofmeasure as quantity_unit_of_measure,
        {{bool('isdeleted')}} as is_deleted,
        {{bool('isarchived')}} as is_archived,
        stockkeepingunit as sku,
        type,
        productclass as product_class,
        sourceproductid as product_id_source,
        sellerid as seller_id

    from source

)

select * from renamed
