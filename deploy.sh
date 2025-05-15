#!/bin/bash

# Exit on any error
set -e

echo "Creating GCP VM instance..."
gcloud compute instances create kcap-assessment-prod \
    --project=inbox-ai-456015 \
    --zone=us-east1-c \
    --machine-type=e2-standard-4 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=inboxai-sa-monitoring-buckets@inbox-ai-456015.iam.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append \
    --tags=http-server,https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=kcap-assessment-prod,image=projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20250508,mode=rw,size=50,type=pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any

echo "Creating firewall rule for Airflow UI on port 8080..."
gcloud compute firewall-rules create allow-airflow-8080 \
    --project=inbox-ai-456015 \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:8080 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=http-server

echo "Waiting for VM to initialize..."
sleep 45

echo "VM and environment setup complete."
