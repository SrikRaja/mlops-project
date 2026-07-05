"""
Apache Airflow DAG - DA5402W Assignment 1, Part C

Workflow: DataGeneration -> DataValidation -> [branch] -> Preprocessing/FeatureEngineering
          -> parallel Analytics helpers -> Analytics -> ReportGeneration

Satisfies:
  1-2. DAG with the 6 required tasks + dependencies
  3. Parallel execution: ComputeSummaryStatistics / ComputeCorrelationAnalysis run concurrently
  4. TaskGroups: PreprocessingGroup, AnalyticsGroup
  5. Retries = 3, retry_delay = 1 minute (default_args)
  6. BranchPythonOperator: check_data_quality
  7. Priority weights on upstream/critical tasks
  8. Scheduled every 5 minutes
"""

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "da25m624",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


# ----------------------------------------------------------------------
# Task callables
# ----------------------------------------------------------------------
def generate_data(**context):
    record_count = random.randint(500, 1500)
    print(f"[DataGeneration] Generated {record_count} synthetic records.")
    context["ti"].xcom_push(key="record_count", value=record_count)


def validate_data(**context):
    record_count = context["ti"].xcom_pull(
        key="record_count", task_ids="DataGeneration")
    # Simulate a data-quality check: ~80% of runs pass
    is_valid = random.random() < 0.80
    print(f"[DataValidation] record_count={record_count}  valid={is_valid}")
    context["ti"].xcom_push(key="is_valid", value=is_valid)
    return is_valid


def check_data_quality(**context):
    """BranchPythonOperator callable — Task 6."""
    is_valid = context["ti"].xcom_pull(
        key="is_valid", task_ids="DataValidation")
    if is_valid:
        return "PreprocessingGroup.DataPreprocessing"
    return "handle_invalid_data"


def handle_invalid_data(**context):
    print("[handle_invalid_data] Data failed validation — skipping preprocessing/analytics, "
          "routing straight to ReportGeneration with a failure notice.")


def preprocess_data(**context):
    print("[DataPreprocessing] Cleaning, imputing missing values, removing invalid records...")


def engineer_features(**context):
    print("[FeatureEngineering] Deriving hour_of_day, day_of_week, weekend_indicator features...")


def compute_summary_statistics(**context):
    print("[ComputeSummaryStatistics] Computing mean/median/std across features...")


def compute_correlation_analysis(**context):
    # Deliberately flaky task to demonstrate Airflow's retry mechanism
    if random.random() < 0.35:
        raise RuntimeError(
            "Simulated transient failure in correlation computation")
    print("[ComputeCorrelationAnalysis] Computing feature correlation matrix...")


def run_analytics(**context):
    print("[Analytics] Aggregating summary statistics + correlation results into final analytics output.")


def generate_report(**context):
    is_valid = context["ti"].xcom_pull(
        key="is_valid", task_ids="DataValidation")
    if is_valid:
        print("[ReportGeneration] Full pipeline report generated successfully.")
    else:
        print(
            "[ReportGeneration] Failure report generated — upstream data validation failed.")


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="assignment1_data_workflow_da25m624",
    description="DA5402W Assignment 1 Part C - Workflow Orchestration",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="*/5 * * * *",   # Task 8: every 5 minutes
    catchup=False,
    tags=["da5402w", "assignment1", "part-c"],
) as dag:

    data_generation = PythonOperator(
        task_id="DataGeneration",
        python_callable=generate_data,
        priority_weight=10,
    )

    data_validation = PythonOperator(
        task_id="DataValidation",
        python_callable=validate_data,
        priority_weight=10,
    )

    branch_quality_check = BranchPythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
        priority_weight=8,
    )

    handle_invalid = PythonOperator(
        task_id="handle_invalid_data",
        python_callable=handle_invalid_data,
    )

    with TaskGroup("PreprocessingGroup") as preprocessing_group:
        data_preprocessing = PythonOperator(
            task_id="DataPreprocessing",
            python_callable=preprocess_data,
            priority_weight=6,
        )
        feature_engineering = PythonOperator(
            task_id="FeatureEngineering",
            python_callable=engineer_features,
            priority_weight=6,
        )
        data_preprocessing >> feature_engineering

    with TaskGroup("AnalyticsGroup") as analytics_group:
        summary_stats = PythonOperator(
            task_id="ComputeSummaryStatistics",
            python_callable=compute_summary_statistics,
        )
        correlation_analysis = PythonOperator(
            task_id="ComputeCorrelationAnalysis",
            python_callable=compute_correlation_analysis,
        )
        analytics = PythonOperator(
            task_id="Analytics",
            python_callable=run_analytics,
            priority_weight=5,
        )
        # Task 3: parallel execution -- both feed into Analytics
        [summary_stats, correlation_analysis] >> analytics

    report_generation = PythonOperator(
        task_id="ReportGeneration",
        python_callable=generate_report,
        priority_weight=5,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------------- Dependencies ----------------
    data_generation >> data_validation >> branch_quality_check
    branch_quality_check >> preprocessing_group >> analytics_group >> report_generation
    branch_quality_check >> handle_invalid >> report_generation
