#!/bin/bash
ENV=$1
CLIENT=$2
PROJECT=$3

TABLES=(
    "pyr_contact_categories"
    "pyr_contact_emails"
    "pyr_contact_phone_numbers"
    "pyr_contacts"
    "pyr_contacts_contact_categories"
    "pyr_ecards"
    "pyr_events"
    "pyr_message_recipients"
    "pyr_messages"
    "pyr_microsites"
    "pyr_rank_definitions"
    "pyr_resource_assets"
    "pyr_resources"
    "pyr_sites"
    "pyr_sms"
    "pyr_user_tasks"
    "tree_commission_bonuses"
    "tree_commission_details"
    "tree_commission_runs"
    "tree_commissions"
    "tree_order_items"
    "tree_order_statuses"
    "tree_order_types"
    "tree_orders"
    "tree_user_statuses"
    "tree_user_types"
    "tree_users"
    "users"
    "pyr_login_histories"
)

echo "Running jobs for ${CLIENT} in ${ENV}"

for t in ${TABLES[@]}; do
    echo "-- Scheduling job for table ${t} --"
    echo ""
    gcloud dataflow jobs run vibe-to-lake-${CLIENT}-${t} \
    --project=${PROJECT} \
    --region=us-central1 \
    --gcs-location=gs://${PROJECT}-dataflow/templates/load_vibe_to_lake \
    --staging-location=gs://${PROJECT}-dataflow/staging \
    --parameters=env=${ENV},client=${CLIENT},dest=${PROJECT}:lake.${t},table=${PROJECT}.pyr_${CLIENT}_${ENV}.${t} \
    --worker-machine-type=n1-standard-2
done