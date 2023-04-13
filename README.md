# Confluent Cloud Chargeback Calculator

This project helps to generate detailed billing data on a per Service Account/User basis and splits cost into 2 major categories:

- Shared Cost -- This is the cost that all users will share in the CCloud Environment/Cluster.
- Usage Cost -- This is the cost that is dependent on usage and is calculated by merging stats from Billing data with Metrics API & the existing SA/Users/Clusters in CCloud.

The codebase generates 3 different types of datasets:

- Hourly -- This is the fine grained cost split on a per hour basis for every entity using the CCloud environment.
- Daily -- This is a rollup from hourly into daily so that no additional calculations are required for per day details.
- Monthly -- This is a monthly rollup but is generally sufficient for Summary details for the entire org entity based split and consumption.

To understand how every Type of cost is split check the `compute_output` method inside the file `src/data_processing/billing_chargeback_processing.py`.
Every type has a defined goal by how the calculations are performed and split for usage and shared cost.
