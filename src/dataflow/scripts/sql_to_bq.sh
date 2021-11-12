#!/bin/bash

ENV=$1
CLIENT=$2
PROJECT=$3
QUIET=$4

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
  "tree_bonuses"
  "pyr_login_histories"
)

for t in ${TABLES[@]}; do
  if [ "${QUIET}" != "y" ]; then
    echo "Schedule job for '${t}'? y/n/c"
    read reply

    case $reply in
      "y")
        echo "-- Scheduling job for table '${t}' --"
        echo ""
        ;;
      "n")
        continue
        ;;
      *)
        break;
        ;;
    esac
  fi

  if [ $t = "tree_commissions" ]; then
    KEY="tree_user_id"
  else
    KEY="id"
  fi

  gcloud dataflow jobs run load-${CLIENT}-${t}-sql-to-bq \
  --project=${PROJECT} \
  --gcs-location=gs://${PROJECT}-dataflow/templates/load_sql_to_bq \
  --staging-location=gs://${PROJECT}-dataflow/staging \
  --parameters=env=${ENV},client=${CLIENT},dest=${PROJECT}:pyr_${CLIENT}_${ENV}.${t},table=${t},key_field=${KEY} \
  --worker-machine-type=n1-standard-2
done