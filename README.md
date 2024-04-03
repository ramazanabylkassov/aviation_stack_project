# Departure Delay Diagnosis

Welcome to the "Departure Delay Diagnosis" Data Engineering project aimed at analyzing and addressing departure delays in three cities of republican significance in Kazakhstan: Astana (NQZ), Almaty (ALA), and Shymkent (CIT).

Departure delays pose significant challenges for passengers, necessitating comprehensive analysis and resolution.

This project utilizes data provided by the [Aviation Stack website](https://aviationstack.com/) and is structured to achieve the following objectives:

1. **Data Upload**: Utilize a [data load tool](https://dlthub.com/) to upload data from the Aviation Stack API to Google Cloud Storage.
  
2. **Data Transformation and Loading**: Extract data from Google Cloud Storage, transform it, and load it into separate tables within a raw data dataset in Google BigQuery.
  
3. **Data Integration**: Combine all relevant data, perform necessary cleaning, and load it into a data mart dataset within Google BigQuery.
  
4. **Visualization and Analysis**: Showcase analytical statistics and visualizations regarding departure delays through a Google Studio Looker dashboard.

By executing these steps, the project aims to provide insights into departure delays in the specified cities and facilitate informed decision-making for mitigating delays and improving passenger experiences.