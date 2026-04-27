-- ============================================================
-- PFC 2.0 — pfc_config
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Descripción: Configuración centralizada de parámetros por país
-- ============================================================

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_config` AS
SELECT
  'PY_PE' AS global_entity_id,
  'pe' AS country_code,
  'date_warehouse_sku' AS join_strategy,
  TRUE AS require_discount_to_charge,
  'skip' AS missing_contract_fallback,
  'normalized' AS funding_value_convention,
  'negotiated' AS funding_source,
  'order_date' AS param_billing_period,
  TRUE AS is_active,
  CURRENT_TIMESTAMP() AS updated_at
UNION ALL
SELECT
  'TB_BH',
  'bh',
  'date_warehouse_sku',
  TRUE,
  'skip',
  'per_benefit',
  'negotiated',
  'campaign_end_date',
  TRUE,
  CURRENT_TIMESTAMP()
UNION ALL
SELECT
  'TB_AE',
  'ae',
  'date_warehouse_sku',
  TRUE,
  'skip',
  'per_benefit',
  'negotiated',
  'campaign_end_date',
  TRUE,
  CURRENT_TIMESTAMP()
