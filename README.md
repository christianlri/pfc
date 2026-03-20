# Supplier Promo Funding Calculator

Requesting, calculating, and collecting supplier funding is challenging for finance and commercial teams due to the manual workflow, which creates risks of potential leakage due to ineffective follow-up and process inefficiencies.
Business WorkFlow: https://docs.google.com/presentation/d/1YS1xw3Wj43Dlnwq4679NqnV4hmsEV0R5U-T2Zj3ZHqc/edit?slide=id.g20311b00b12_0_59#slide=id.g20311b00b12_0_59

This DAG aims to automate the supplier funding calculation process to ensure that credit notes associated with promotions are collected promptly and efficiently.

This DAG has varied scenarios for campaigns that are to be considered for calculation in the schedule, please read the following section for the details.

## Supplier Funding incremental SQL date filtering logic

Below section explains a BigQuery SQL Common Table Expression (CTE) structure designed to dynamically calculate `filter_start_date` and `filter_end_date` based on a given `input_date`. The logic handles specific scenarios related to the 1st day of the month and the first Monday following it, providing a flexible date range for filtering campaigns or other time-series data.

** TO DO ** To move step#1 to a UDF.

---

### 1. Purpose

The primary purpose of this SQL code is to generate a precise date range (`filter_start_date` and `filter_end_date`) that can be used in the filter criteria i.e. in the `WHERE` clause (e.g., `campaign_end_date BETWEEN filter_start_date AND filter_end_date`).
This range adapts based on whether the `input_date` is the 1st of the month, and specifically if it's the first Monday *after* the 1st of the month.

### 2. Input

* **`{{ next_ds }}`**: This is a airflow macro that represents the logical execution date of the DAG, expected in `YYYY-MM-DD` string format. This string is converted to a `DATE` data type using `DATE('{{ next_ds }}')`.

### 3. Output

The final CTE, `filtered_dates`, produces two columns:

* **`filter_start_date`**: The calculated start date for your filtering range.
* **`filter_end_date`**: The calculated end date for your filtering range.

---

### 4. Logic Breakdown (CTEs)

The code is structured using three CTEs to break down the logic into manageable steps:

#### 4.1. `date_context` CTE

This initial CTE calculates fundamental date components from the `input_date`.

* **`input_date`**: The date provided by `{{ next_ds }}`, converted to a `DATE` type.
* **`current_week_monday`**: The date of the Monday of the week that `input_date` falls into. This is calculated using `DATE_TRUNC(input_date, WEEK(MONDAY))`.
* **`first_day_of_input_month`**: The 1st day of the month for the `input_date`. This is calculated using `DATE_TRUNC(input_date, MONTH)`.

#### 4.2. `scenario_flags` CTE

This CTE builds upon `date_context` to identify specific date scenarios using boolean flags. This is where the core conditional logic for your custom date ranges is defined.

* **`is_first_of_month_not_monday`**:
    * **TRUE** if: The `input_date` is the 1st day of the month AND the `input_date` is NOT a Monday.
    * **FALSE** otherwise.
* **`is_first_monday_after_first`**:
    * **TRUE** if: The `input_date` is a Monday AND it is strictly *after* the 1st day of the month AND it falls within the first 7 days *from* the 1st of the month.
    * **FALSE** otherwise.
    * *Note:* This flag is designed to be `FALSE` if the `input_date` *is* the 1st of the month and also a Monday. This ensures that the "default" behavior (previous week) applies in that specific edge

#### 4.3. `filtered_dates` CTE

This final CTE uses the flags from `scenario_flags` to determine the `filter_start_date` and `filter_end_date` based on a `CASE` statement. The conditions are ordered to prioritize the specific scenarios.

* **`filter_start_date` Logic:**
    * If `is_first_of_month_not_monday` is TRUE: `filter_start_date` is `current_week_monday` (Monday of the input date's week).
    * Else if `is_first_monday_after_first` is TRUE: `filter_start_date` is `first_day_of_input_month` (1st of the input month).
    * Else (default behavior): `filter_start_date` is `current_week_monday - 7 days` (Monday of the previous week).
* **`filter_end_date` Logic:**
    * If `is_first_of_month_not_monday` is TRUE: `filter_end_date` is `LAST_DAY(input_date, MONTH)` (the last day of the input month).
    * Else if `is_first_monday_after_first` is TRUE: `filter_end_date` is `input_date - 1 day` (the Sunday immediately preceding the current Monday).
    * Else (default behavior): `filter_end_date` is `current_week_monday - 1 day` (Sunday of the previous week).

---

### 5. Scenario Examples

Let's illustrate the behavior with different `input_date` examples:

| `input_date` (Example) | Day of Week | `is_first_of_month_not_monday` | `is_first_monday_after_first` | `filter_start_date` | `filter_end_date` | Explanation |
| :--------------------- | :---------- | :----------------------------- | :---------------------------- | :------------------ | :---------------- | :---------- |
| **`2025-07-16`** | Wednesday | FALSE | FALSE | `2025-07-07` | `2025-07-13` | **Default Behavior:** Not 1st of month. Filters previous Monday to previous Sunday. (`current_week_monday` was 2025-07-14) |
| **`2025-07-01`** | Tuesday | TRUE | FALSE | `2025-06-30` | `2025-07-31` | **1st of Month, Not Monday:** Filters from current week's Monday to month-end. (`current_week_monday` was 2025-06-30) |
| **`2025-09-01`** | Monday | FALSE | FALSE | `2025-08-25` | `2025-08-31` | **1st of Month, IS Monday:** Falls into Default Behavior. Filters previous Monday to previous Sunday. (`current_week_monday` was 2025-09-01) |
| **`2025-09-08`** | Monday | FALSE | TRUE | `2025-09-01` | `2025-09-07` | **First Monday AFTER 1st of Month:** Filters from 1st of month to most recent Sunday. (`current_week_monday` was 2025-09-08) |

### 6. Usage

To use this logic in your main BigQuery SQL query, you would typically:

1.  Include the `date_context`, `scenario_flags`, and `filtered_dates` CTEs at the beginning of your script.
2.  Join your target table (e.g., `your_campaigns_table`) with the `filtered_dates` CTE.
3.  Apply the `BETWEEN` clause using `filter_start_date` and `filter_end_date` in your `WHERE` condition.

**Example Integration:**

```
SELECT
  c.* -- Select all columns from your campaigns table
FROM `fulfillment-dwh-production.curated_data_shared_dmart.qc_campaigns` AS c
  INNER JOIN filtered_dates AS fr
  ON c.campaign_end_date BETWEEN fr.filter_start_date AND fr.filter_end_date;
```