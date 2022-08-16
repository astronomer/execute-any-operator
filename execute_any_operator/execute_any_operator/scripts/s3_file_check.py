from airflow.exceptions import AirflowSensorTimeout
from execute_any_operator.operators.execute_any import ExecuteAnyOperator

hello = ExecuteAnyOperator(
    operator="airflow.operators.bash:BashOperator", bash_command="echo 'Hello, World!'"
)
hello.execute()

# kpo = ExecuteAnyOperator(
#     operator="KubernetesPodOperator",
#     namespace="default",
#     image='alpine',
#     cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
#     name="write-xcom",
#     do_xcom_push=True,
#     get_logs=True,
#     in_cluster=False
# )
# kpo.execute()

s3_check = ExecuteAnyOperator(
    operator="airflow.providers.amazon.aws.sensors.s3_key:S3KeySensor",
    bucket_name="dylanintorf-dev",
    bucket_key="dylan-dev/DagRun.pickles",
    poke_interval=1,
    timeout=5,
)
try:
    s3_check.execute()
    print("File exists!")
except AirflowSensorTimeout:
    print("File not found")
