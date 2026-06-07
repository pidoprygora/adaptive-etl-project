.PHONY: init plan apply destroy ssh ssm sync-dags watch-dags logs setup-athena bootstrap-s3-prefixes debug-airflow hosts-airflow patch-aws-env patch-aws-env-ssh

init:
	terraform init

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

ssh:
	ssh -i $${SSH_KEY_PATH:-$$HOME/.ssh/id_rsa} \
	  ec2-user@$$(terraform output -raw instance_public_ip)

ssm:
	aws ssm start-session --target $$(terraform output -raw instance_id)

sync-dags:
	./scripts/sync_dags.sh

watch-dags:
	./scripts/sync_dags.sh --watch

logs:
	ssh -i $${SSH_KEY_PATH:-$$HOME/.ssh/id_rsa} \
	  -o StrictHostKeyChecking=no \
	  ec2-user@$$(terraform output -raw instance_public_ip) \
	  "sudo tail -f /var/log/user-data.log"

# Run ONCE before the first DAG execution.
# Creates all 20 processed target tables in Glue via Athena DDL.
setup-athena:
	./scripts/setup_athena.sh

bootstrap-s3-prefixes:
	python3 ../../src/s3_location_bootstrap.py --all

debug-airflow:
	./scripts/debug_airflow.sh 

hosts-airflow:
	./scripts/update_local_hosts_airflow.sh airflow.lab

patch-aws-env:
	./scripts/patch_airflow_aws_env.sh

patch-aws-env-ssh:
	scp -i $${SSH_KEY_PATH:-$$HOME/.ssh/id_rsa} \
	  -o StrictHostKeyChecking=no \
	  ./scripts/patch_airflow_aws_env.sh \
	  ec2-user@$$(terraform output -raw instance_public_ip):/tmp/patch_airflow_aws_env.sh
	ssh -i $${SSH_KEY_PATH:-$$HOME/.ssh/id_rsa} \
	  -o StrictHostKeyChecking=no \
	  ec2-user@$$(terraform output -raw instance_public_ip) \
	  "chmod +x /tmp/patch_airflow_aws_env.sh && export AWS_ACCESS_KEY_ID='$$AWS_ACCESS_KEY_ID' AWS_SECRET_ACCESS_KEY='$$AWS_SECRET_ACCESS_KEY' AWS_SESSION_TOKEN='$$AWS_SESSION_TOKEN' AWS_REGION='$${AWS_REGION:-eu-north-1}' && /tmp/patch_airflow_aws_env.sh"