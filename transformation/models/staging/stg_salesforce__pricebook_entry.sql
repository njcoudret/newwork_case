

with source as (

    select * from {{ source('salesforce', 'pricebook_entry') }}

),

renamed as (

    select
        id as pricebook_entry_id,
        {{set_null('pricebook2id')}} as price_book_id,
        {{set_null('product2id')}} as product_id,
        unitprice as unit_price,
        {{bool('isactive')}} as is_active,
        {{bool('usestandardprice')}} as use_standard_price,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{bool('isdeleted')}} as is_deleted,
        {{bool('isarchived')}} as is_archived

    from source

)

select * from renamed
