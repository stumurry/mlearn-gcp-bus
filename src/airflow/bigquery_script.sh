#!/bin/bash
​
jeeves bigquery_migration down local --version 20201116162413_create_lead_match_score_table.py
jeeves bigquery_migration up local --version 20201116162413_create_lead_match_score_table.py
​
jeeves bigquery_migration down local --version 20201116162440_create_lead_persuasion_table.py
jeeves bigquery_migration up local --version 20201116162440_create_lead_persuasion_table.py
​
jeeves bigquery_migration down local --version 20201116162448_create_lead_channel_table.py
jeeves bigquery_migration up local --version 20201116162448_create_lead_channel_table.py
​
jeeves bigquery_migration down local --version 20201116180359_create_entities_table.py
jeeves bigquery_migration up local --version 20201116180359_create_entities_table.py
​
jeeves bigquery_migration down local --version 20201125183337_create_lead_relationship_fit_table.py
jeeves bigquery_migration up local --version 20201125183337_create_lead_relationship_fit_table.py

jeeves bigquery_migration up local --version 20210111224416_create_lake_contacts.py

jeeves bigquery_migration down local --version 20191202214031_create_contacts_table.py
jeeves bigquery_migration up local --version 20191202214031_create_contacts_table.py

jeeves bigquery_migration down local --version 20201109210318_create_lead_files_tracking_table.py
jeeves bigquery_migration up local --version 20201109210318_create_lead_files_tracking_table.py

jeeves bigquery_migration down local --version 20210127162413_create_adopt_curve_table.py
jeeves bigquery_migration up local --version 20210127162413_create_adopt_curve_table.py


