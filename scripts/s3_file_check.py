from util.execute_any_operator import ExecuteAnyOperator

echo_hello = ExecuteAnyOperator(operator="BashOperator", bash_command="echo 'Hello, World!'")
s3_check = ExecuteAnyOperator(operator="S3KeySensor", bucket_name="dylanintorf-dev", bucket_key="dylan-dev/DagRun.pickle", poke_interval=1, timeout=5)
s3_check.execute()
