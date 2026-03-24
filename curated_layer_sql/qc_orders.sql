WITH qc_order_lines AS (

  SELECT
    qc_ol.global_entity_id
    , qc_ol.order_id
    , ARRAY_AGG(
        STRUCT(
          qc_ol.sku
          , qc_ol.global_catalog_id
          , qc_ol.platform_product_id
          , qc_ol.pelican_order_item_id
          , qc_ol.djini_order_item_id
          , qc_ol.replacement_pelican_order_item_id
          , qc_ol.min_quantity
          , qc_ol.max_quantity
          , qc_ol.pricing_type
          , qc_ol.product_vat_rate
          , qc_ol.sales_buffer
          , qc_ol.quantity_ordered
          , qc_ol.quantity_picked_up
          , qc_ol.quantity_delivered
          , qc_ol.quantity_returned
          , qc_ol.quantity_sold
          , qc_ol.pickup_issue
          , qc_ol.pelican_order_item_status
          , qc_ol.is_custom
          , qc_ol.is_modified_quantity
          , qc_ol.is_modified_price
          , qc_ol.is_checkout_confirmed
          , qc_ol.campaign_info
          , qc_ol.ordered_weight
          , qc_ol.delivered_weight
          , qc_ol.sold_weight
          , qc_ol.returned_weight
          , qc_ol.weight_unit
          , qc_ol.weighted_pieces_ordered
          , qc_ol.weighted_pieces_picked_up
          , STRUCT(
              qc_ol.unit_price_listed_eur
              , qc_ol.unit_price_eur
              , qc_ol.unit_price_paid_eur
              , qc_ol.unit_discount_eur
              , qc_ol.djini_order_items_discount_eur
              , qc_ol.total_amt_paid_eur
              , qc_ol.total_amt_paid_net_eur
              , qc_ol.unit_cost_eur
              , qc_ol.amt_cogs_eur
              , qc_ol.djini_order_items_supplier_funded_eur
              , qc_ol.order_items_supplier_funded_eur
              , qc_ol.total_ppp_eur
            ) AS value_euro
          , STRUCT(
              qc_ol.unit_price_listed_lc
              , qc_ol.unit_price_lc
              , qc_ol.unit_price_paid_lc
              , qc_ol.unit_discount_lc
              , qc_ol.djini_order_items_discount_lc
              , qc_ol.total_amt_paid_lc
              , qc_ol.total_amt_paid_net_lc
              , qc_ol.unit_cost_lc
              , qc_ol.amt_cogs_lc
              , qc_ol.djini_order_items_supplier_funded_lc
              , qc_ol.order_items_supplier_funded_lc
              , qc_ol.total_ppp_lc
            ) AS value_lc
          , qc_ol.returns
          , qc_ol.combo_products
          , qc_ol.weightable_attributes
        )
      ) AS items
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._qc_order_line_products` AS qc_ol
  WHERE TRUE
    {%- if not params.backfill %}
    AND qc_ol.order_created_date_utc BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
      AND
      DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND qc_ol.order_created_date_utc BETWEEN
      DATE_SUB('{{ params.backfill_start_date }}', INTERVAL 1 DAY)
      AND
      DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}
  GROUP BY 1, 2

), qc_orders AS (

  SELECT
    qc_o.order_created_date_utc
    , qc_o.order_created_at_utc
    , qc_o.order_created_date_lt
    , qc_o.order_created_at_lt
    , qc_o.global_entity_id
    , qc_o.fulfilled_by_entity
    , qc_o.order_id
    , qc_o.checkout_invoice_number
    , qc_o.platform_vendor_id
    , qc_o.vertical_type
    , qc_o.vertical_parent
    , qc_o.country_code
    , qc_o.currency_code
    , qc_o.warehouse_id
    , qc_o.customer_order_rank
    , qc_o.order_updated_at_utc
    , qc_o.is_failed_before_transmission
    , qc_o.is_dmart
    , qc_o.origin
    , qc_o.is_delivery_by_dh
    , qc_o.customer_id
    , qc_o.analytical_customer_id
    , qc_o.payment_method
    , qc_o.payment_provider
    , qc_o.payment_type
    , qc_o.order_status
    , qc_o.picking_time_seconds
    , qc_o.picking_time_idle_start
    , qc_o.failure_owner
    , qc_o.failure_reason
    , qc_o.failure_stage
    , qc_o.failure_source
    , qc_o.is_pickup
    , qc_o.is_billable
    , qc_o.is_successful
    , qc_o.is_cancelled
    , qc_o.is_failed
    , qc_o.is_acquisition
    , qc_o.is_voucher_applied
    , qc_o.is_preorder
    , qc_o.is_mobile
    , qc_o.device_group
    , qc_o.is_free_delivery
    , qc_o.is_modified_order
    , qc_o.pelican_order_status
    , qc_o.pelican_checkout_confirmed_total_lc
    , qc_o.quantity_ordered
    , qc_o.quantity_picked_up
    , qc_o.quantity_delivered
    , qc_o.quantity_returned
    , qc_o.quantity_sold
    , qc_o.amt_payment_total_gross_lc
    , qc_o.amt_delivery_fee_pelican_lc
    , qc_o.delivery_time_state
    , qc_o.actual_delivery_time_minutes
    , qc_o.promised_delivery_time_minutes
    , qc_o.delivery_delay_minutes
    , qc_o.dropoff_distance_manhattan
    , qc_o.delivery_vehicle_name
    , qc_o.number_of_bags
    , qc_o.order_package_total_volume
    , qc_o.order_package_total_weight
    , qc_o.is_split_order
    , qc_o.invoice_date_utc
    , qc_o.invoice_number
    , qc_o.has_discount_campaign
    , qc_o.has_brand_campaign
    , qc_o.parent_order_id
    --TODO: QCDAE-1238 Deprecate column `djini_order_discount_lc` in favor of `order_discount_lc`
    , qc_o.djini_order_discount_lc
    , qc_o.order_discount_lc
    --TODO: QCDAE-1238 Deprecate column `djini_order_discount_eur` in favor of `order_discount_eur`
    , qc_o.djini_order_discount_eur
    , qc_o.order_discount_eur
    , qc_o.djini_order_supplier_funded_lc
    , qc_o.djini_order_supplier_funded_eur
    , qc_o.order_supplier_funded_lc
    , qc_o.order_supplier_funded_eur
    , qc_o.total_amt_paid_lc
    , qc_o.total_amt_paid_eur
    , qc_o.total_amt_paid_net_lc
    , qc_o.total_amt_paid_net_eur
    , qc_o.amt_cogs_lc
    , qc_o.amt_cogs_eur
    , qc_o.total_ppp_lc
    , qc_o.total_ppp_eur
    , qc_o.amt_gbv_lc
    , qc_o.amt_gbv_eur
    , qc_o.amt_gmv_lc
    , qc_o.amt_gmv_eur
    -- Internal GMV components in EUR and local currency
    , qc_o.total_order_value_eur
    , qc_o.total_order_value_lc
    , qc_o.delivery_tip_eur
    , qc_o.delivery_tip_lc
    , qc_o.customer_paid_wallet_eur
    , qc_o.customer_paid_wallet_lc
    -- Delivery fee in local currency and in Euros
    , qc_o.amt_delivery_fee_lc
    , qc_o.amt_delivery_fee_eur
    , qc_o.amt_delivery_fee_before_discount_lc
    , qc_o.amt_delivery_fee_before_discount_eur
    -- Service fee in EUR and local currency
    , qc_o.amt_service_fee_lc
    , qc_o.amt_service_fee_eur
    -- Commission fee in local currency and in Euros
    , qc_o.amt_commission_lc
    , qc_o.amt_commission_estimated_lc
    , qc_o.amt_commission_eur
    , qc_o.amt_commission_estimated_eur
    -- Discount in local currency and in Euros
    , qc_o.amt_discount_lc
    , qc_o.amt_discount_dh_lc
    , qc_o.amt_discount_other_lc
    , qc_o.amt_discount_eur
    , qc_o.amt_discount_dh_eur
    , qc_o.amt_discount_other_eur
    -- Voucher in local currency and in Euros
    , qc_o.amt_voucher_lc
    , qc_o.amt_voucher_dh_lc
    , qc_o.amt_voucher_other_lc
    , qc_o.amt_voucher_eur
    , qc_o.amt_voucher_dh_eur
    , qc_o.amt_voucher_other_eur
    -- Minimum order value charge in EUR and local currency
    , qc_o.amt_mov_customer_fee_eur
    , qc_o.amt_mov_customer_fee_lc
    -- Order profit
    , CAST(qc_o.amt_profit_lc AS FLOAT64) AS amt_profit_lc
    , qc_o.amt_profit_eur
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._qc_orders` AS qc_o
  WHERE TRUE
    {%- if not params.backfill %}
    AND qc_o.order_created_date_lt BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
      AND
      DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND qc_o.order_created_date_lt BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), qc_orders_rank AS (

  SELECT
    qc_or.global_entity_id
    , qc_or.analytical_customer_id
    , qc_or.order_id
    , qc_or.customer_qc_order_rank
    , qc_or.is_acquisition_qcommerce
    , qc_or.is_acquisition_darkstore
    , qc_or.is_acquisition_localshop
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._qc_order_ranking` AS qc_or
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(qc_or.partition_month_local) BETWEEN
      DATE_TRUNC(DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY), MONTH)
      AND
      DATE_TRUNC(DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY), MONTH)
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(qc_or.partition_month_local) BETWEEN
      DATE_TRUNC(DATE_SUB('{{ params.backfill_start_date }}', INTERVAL 1 DAY), MONTH)
      AND
      DATE_TRUNC(DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY), MONTH)
    {%- endif %}

)
SELECT
  qc_o.order_created_date_utc
  , qc_o.order_created_date_lt
  , qc_o.global_entity_id
  , qc_o.fulfilled_by_entity
  , qc_o.origin
  , qc_o.currency_code
  , qc_o.country_code
  , qc_o.vertical_type
  , qc_o.is_dmart
  , qc_o.platform_vendor_id
  , qc_o.is_delivery_by_dh
  , qc_o.customer_id
  , qc_o.analytical_customer_id
  , qc_o.customer_order_rank
  , qc_or.customer_qc_order_rank
  , qc_o.order_id
  , qc_o.order_created_at_utc
  , qc_o.order_created_at_lt
  , qc_o.order_updated_at_utc
  , qc_o.payment_method
  , qc_o.payment_provider
  , qc_o.payment_type
  , qc_o.invoice_number
  , qc_o.invoice_date_utc
  , qc_o.pelican_order_status
  , qc_o.order_status
  , qc_o.parent_order_id
  , qc_o.is_successful
  , qc_o.is_acquisition
  , qc_o.has_discount_campaign
  , qc_o.has_brand_campaign
  , COALESCE(qc_or.is_acquisition_qcommerce, FALSE) AS is_acquisition_qcommerce
  , COALESCE(qc_or.is_acquisition_darkstore, FALSE) AS is_acquisition_darkstore
  , COALESCE(qc_or.is_acquisition_localshop, FALSE) AS is_acquisition_localshop
  , qc_o.is_voucher_applied
  , qc_o.is_preorder
  , qc_o.is_mobile
  , qc_o.device_group
  , qc_o.is_failed
  , qc_o.is_cancelled
  , qc_o.failure_owner
  , qc_o.failure_reason
  , qc_o.failure_stage
  , qc_o.failure_source
  , qc_o.is_pickup
  , qc_o.is_billable
  , qc_o.is_failed_before_transmission
  , qc_o.is_modified_order
  , qc_o.delivery_time_state
  , qc_o.is_free_delivery
  , qc_o.quantity_ordered
  , qc_o.quantity_picked_up
  , qc_o.quantity_delivered
  , qc_o.quantity_returned
  , qc_o.quantity_sold
  , qc_o.checkout_invoice_number
  , qc_o.warehouse_id
  , qc_o.picking_time_seconds
  , qc_o.picking_time_idle_start
  , qc_o.actual_delivery_time_minutes
  , qc_o.promised_delivery_time_minutes
  , qc_o.delivery_delay_minutes
  , qc_o.dropoff_distance_manhattan
  , qc_o.delivery_vehicle_name
  , qc_o.number_of_bags
  , qc_o.order_package_total_volume
  , qc_o.order_package_total_weight
  , qc_o.is_split_order
  , qc_o.amt_payment_total_gross_lc
  , qc_o.amt_delivery_fee_pelican_lc
  , STRUCT(
      qc_o.amt_gmv_eur
      , qc_o.total_order_value_eur
      , qc_o.delivery_tip_eur
      , qc_o.customer_paid_wallet_eur
      , qc_o.amt_commission_eur
      , qc_o.amt_commission_estimated_eur
      , qc_o.amt_delivery_fee_eur
      , qc_o.amt_delivery_fee_before_discount_eur
      , qc_o.amt_service_fee_eur
      , qc_o.amt_mov_customer_fee_eur
      , qc_o.amt_discount_eur
      , qc_o.amt_discount_dh_eur
      , qc_o.amt_discount_other_eur
      --TODO: QCDAE-1238 Deprecate column `djini_order_discount_eur` in favor of `order_discount_eur`
      , qc_o.djini_order_discount_eur
      , qc_o.order_discount_eur
      , qc_o.djini_order_supplier_funded_eur
      , qc_o.order_supplier_funded_eur
      , qc_o.amt_voucher_eur
      , qc_o.amt_voucher_dh_eur
      , qc_o.amt_voucher_other_eur
      , CAST(qc_o.amt_cogs_eur AS NUMERIC) AS amt_cogs_eur
      , qc_o.total_amt_paid_eur
      , qc_o.total_amt_paid_net_eur
      , CAST(qc_o.amt_profit_eur AS NUMERIC) AS amt_profit_eur
      , CAST(qc_o.total_ppp_eur AS NUMERIC) AS total_ppp_eur
      , qc_o.amt_gbv_eur
    ) AS order_value_euro
  , STRUCT(
      qc_o.amt_gmv_lc
      , qc_o.total_order_value_lc
      , qc_o.delivery_tip_lc
      , qc_o.customer_paid_wallet_lc
      , qc_o.amt_commission_lc
      , qc_o.amt_commission_estimated_lc
      , qc_o.amt_delivery_fee_lc
      , qc_o.amt_delivery_fee_before_discount_lc
      , qc_o.amt_service_fee_lc
      , qc_o.amt_mov_customer_fee_lc
      , qc_o.amt_discount_lc
      , qc_o.amt_discount_dh_lc
      , qc_o.amt_discount_other_lc
      --TODO: QCDAE-1238 Deprecate column `djini_order_discount_lc` in favor of `order_discount_lc`
      , qc_o.djini_order_discount_lc
      , qc_o.order_discount_lc
      , qc_o.djini_order_supplier_funded_lc
      , qc_o.order_supplier_funded_lc
      , qc_o.amt_voucher_lc
      , qc_o.amt_voucher_dh_lc
      , qc_o.amt_voucher_other_lc
      , CAST(qc_o.amt_cogs_lc AS NUMERIC) AS amt_cogs_lc
      , qc_o.total_amt_paid_lc
      , qc_o.total_amt_paid_net_lc
      , CAST(qc_o.amt_profit_lc AS NUMERIC) AS amt_profit_lc
      , CAST(qc_o.total_ppp_lc AS NUMERIC) AS total_ppp_lc
      , qc_o.amt_gbv_lc
      , qc_o.pelican_checkout_confirmed_total_lc
    ) AS order_value_lc
  , qc_ol.items
FROM qc_orders AS qc_o
LEFT JOIN qc_order_lines AS qc_ol
  ON qc_o.global_entity_id = qc_ol.global_entity_id
  AND qc_o.order_id = qc_ol.order_id
LEFT JOIN qc_orders_rank AS qc_or
  ON qc_o.global_entity_id = qc_or.global_entity_id
  AND qc_o.order_id = qc_or.order_id
WHERE TRUE
  {%- if not params.backfill %}
  AND qc_o.order_created_date_lt BETWEEN
    DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
    AND
    DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
  {%- elif params.is_backfill_chunks_enabled %}
  AND qc_o.order_created_date_lt BETWEEN
    '{{ params.backfill_start_date }}'
    AND
    '{{ params.backfill_end_date }}'
  {%- endif %}
