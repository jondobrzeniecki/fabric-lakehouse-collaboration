# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e4ba01b-e0c9-4697-bf28-2669bdf63d6b",
# META       "default_lakehouse_name": "lh1",
# META       "default_lakehouse_workspace_id": "60a1fe4d-1c7c-41bc-9e75-88ecc964a1bd",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e4ba01b-e0c9-4697-bf28-2669bdf63d6b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create, evaluate, and score a churn prediction model
# Updated: Test workflow trigger

# CELL ********************

print('hi')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# For this notebook, you'll install the `imblearn` using `%pip install`. Note that the PySpark kernel will be restarted after `%pip install`, thus you'll need to install the library before you run any other cells.

# CELL ********************

%pip install scikit-learn==1.8.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Load the data

# MARKDOWN ********************

# ### Dataset
# 
# The dataset in *churn.csv* contains the churn status of 10,000 customers, along with 14 attributes that include:
# 
# - Credit score
# - Geographical location (Germany, France, Spain)
# - Gender (male, female)
# - Age
#  Tenure (number of years the person was a customer at that bank)
# - Account balance
# - Estimated salary
# - Number of products that a customer purchased through the bank
# - Credit card status (whether or not the customer has a credit card)
# - Active member status (whether or not the person is an active bank customer)
# 
# The dataset also includes row number, customer ID, and customer surname columns. Values in these columns shouldn't influence a customer's decision to leave the bank.
# 
# A customer bank account closure event defines the churn for that customer. The dataset `Exited` column refers to the customer's abandonment. Since we have little context about these attributes, we don't need background information about the dataset. We want to understand how these attributes contribute to the `Exited` status.
# 
# Out of those 10,000 customers, only 2037 customers (roughly 20%) left the bank. Because of the class imbalance ratio, we recommend generation of synthetic data. Confusion matrix accuracy might not have relevance for imbalanced classification. We might want to measure the accuracy using the Area Under the Precision-Recall Curve (AUPRC).
# 
# - This table shows a preview of the `churn.csv` data:
# 
# |CustomerID|Surname|CreditScore|Geography|Gender|Age|Tenure|Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|
# |---|---|---|---|---|---|---|---|---|---|---|---|---|
# |15634602|Hargrave|619|France|Female|42|2|0.00|1|1|1|101348.88|1|
# |15647311|Hill|608|Spain|Female|41|1|83807.86|1|0|1|112542.58|0|


# MARKDOWN ********************

# ### Download dataset and upload to lakehouse
# 
# Define these parameters, so that you can use this notebook with different datasets:

# CELL ********************

# IS_CUSTOM_DATA = False  # If TRUE, the dataset has to be uploaded manually

# IS_SAMPLE = False  # If TRUE, use only SAMPLE_ROWS of data for training; otherwise, use all data
# SAMPLE_ROWS = 5000  # If IS_SAMPLE is TRUE, use only this number of rows for training


# DATA_ROOT = "/lakehouse/default"
# DATA_FOLDER = "Files/churn"  # Folder with data files
# DATA_FILE = "churn.csv"  # Data file name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# 
# This code downloads a publicly available version of the dataset, and then stores that dataset in a Fabric lakehouse:
# 
# > [!IMPORTANT]
# > [Add a lakehouse](https://aka.ms/fabric/addlakehouse) to the notebook before you run it. Failure to do so will result in an error.

# CELL ********************

# import os, requests
# if not IS_CUSTOM_DATA:
# # With an Azure Synapse Analytics blob, this can be done in one line

# #  Download demo data files into the lakehouse if they don't exist
#     remote_url = "https://synapseaisolutionsa.z13.web.core.windows.net/data/bankcustomerchurn"
#     file_list = ["churn.csv"]
#     download_path = "/lakehouse/default/Files/churn/raw"

#     if not os.path.exists("/lakehouse/default"):
#         raise FileNotFoundError(
#             "Default lakehouse not found, please add a lakehouse and restart the session."
#         )
#     os.makedirs(download_path, exist_ok=True)
#     for fname in file_list:
#         if not os.path.exists(f"{download_path}/{fname}"):
#             r = requests.get(f"{remote_url}/{fname}", timeout=30)
#             with open(f"{download_path}/{fname}", "wb") as f:
#                 f.write(r.content)
#     print("Downloaded demo data files into lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Start recording the time needed to run the notebook:

# CELL ********************

# Record the notebook running time
import time

ts = time.time()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read raw data from the lakehouse
# 
# This code reads raw data from the **Files** section of the lakehouse, and adds more columns for different date parts. Creation of the partitioned delta table uses this information.

# CELL ********************

df = spark.sql("SELECT * FROM lh1.edw_stage.customer_demographics")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create a pandas DataFrame from the dataset
# 
# This code converts the Spark DataFrame to a pandas DataFrame, for easier processing and visualization:

# CELL ********************

df = df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Code generated by Data Wrangler for pandas DataFrame

def clean_data(df):
    # Change column type to int32 for columns: 'RowNumber', 'CustomerId' and 4 other columns
    df = df.astype({'RowNumber': 'int32', 'CustomerId': 'int32', 'CreditScore': 'int32', 'Age': 'int32', 'Tenure': 'int32', 'NumOfProducts': 'int32'})
    # Change column type to float32 for columns: 'Balance', 'EstimatedSalary'
    df = df.astype({'Balance': 'float32', 'EstimatedSalary': 'float32'})
    # Change column type to bool for columns: 'HasCrCard', 'IsActiveMember', 'Exited'
    # df = df.astype({'HasCrCard': 'bool', 'IsActiveMember': 'bool', 'Exited': 'bool'})
    return df

df = clean_data(df.copy())
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_data(df):
    # Drop rows with missing data across all columns
    df.dropna(inplace=True)
    # Drop duplicate rows in columns: 'RowNumber', 'CustomerId'
    df.drop_duplicates(subset=['RowNumber', 'CustomerId'], inplace=True)
    # Drop columns: 'RowNumber', 'CustomerId', 'Surname'
    df.drop(columns=['RowNumber', 'CustomerId', 'Surname'], inplace=True)
    return df

df_clean = clean_data(df.copy())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display df_clean schema
df_clean.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Determine attributes
# 
# This code determines the categorical, numerical, and target attributes:

# CELL ********************

# Determine the dependent (target) attribute
dependent_variable_name = "Exited"
print(dependent_variable_name)
# Determine the categorical attributes
categorical_variables = [col for col in df_clean.columns if col in "O"
                        or df_clean[col].nunique() <=5
                        and col not in "Exited"]
print(categorical_variables)
# Determine the numerical attributes
numeric_variables = [col for col in df_clean.columns if df_clean[col].dtype != "object"
                        and df_clean[col].nunique() >5]
print(numeric_variables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Perform feature engineering 
# 
# 
# This feature engineering generates new attributes based on the current attributes:

# CELL ********************

import pandas as pd

df_clean["NewTenure"] = df_clean["Tenure"]/df_clean["Age"]
df_clean["NewCreditsScore"] = pd.qcut(df_clean['CreditScore'], 6, labels = [1, 2, 3, 4, 5, 6])
df_clean["NewAgeScore"] = pd.qcut(df_clean['Age'], 8, labels = [1, 2, 3, 4, 5, 6, 7, 8])
df_clean["NewBalanceScore"] = pd.qcut(df_clean['Balance'].rank(method="first"), 5, labels = [1, 2, 3, 4, 5])
df_clean["NewEstSalaryScore"] = pd.qcut(df_clean['EstimatedSalary'], 10, labels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = pd.get_dummies(df_clean, columns=['Geography', 'Gender'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create a delta table to generate the Power BI report

# CELL ********************

table_name = "customer_demographics_clean"
# Create a PySpark DataFrame from pandas
sparkDF=spark.createDataFrame(df_clean) 
# write table to dbo lakehouse schema
sparkDF.write.mode("overwrite").saveAsTable(f"dbo.{table_name}")
print(f"Spark DataFrame saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SEED = 12345
# load table dbo.customer_demographics_clean from variable table_name
df_clean = spark.read.table(f"dbo.{table_name}").toPandas()
# df_clean = spark.read.format("delta").load("Tables/df_clean").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import the required libraries for model training
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, confusion_matrix, recall_score, roc_auc_score, classification_report

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Evaluate and save the final machine learning model


# MARKDOWN ********************

# Open the saved experiment from the workspace to select and save the best model.

# CELL ********************


import mlflow

model_uri = f"models:/rfc1_sm/1"
model_info = mlflow.models.get_model_info(model_uri)

# Check the logged environment
print(model_info.flavors)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
import mlflow.pyfunc
# Define run_uri to fetch the model
# MLflow client: mlflow.model.url, list model
# load_model_rfc1_sm = mlflow.sklearn.load_model(f"models:/rfc1_sm/1")
load_model_rfc1_sm = mlflow.pyfunc.load_model(f"models:/rfc1_sm/1")
# load_model_rfc2_sm = mlflow.sklearn.load_model(f"runs:/{rfc2_sm_run_id}/model")
# load_model_lgbm1_sm = mlflow.lightgbm.load_model(f"runs:/{lgbm1_sm_run_id}/model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Assess the performance of the saved models on the testing dataset

# CELL ********************

df_clean = df_clean.drop("Exited",axis=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ypred_rfc1_sm = load_model_rfc1_sm.predict(df_clean) # Random forest with maximum depth of 4 and 4 features
# ypred_rfc2_sm = load_model_rfc2_sm.predict(X_test) # Random forest with maximum depth of 8 and 6 features
# ypred_lgbm1_sm = load_model_lgbm1_sm.predict(X_test) # LightGBM

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save results for Power BI
# 
# Save the delta frame to the lakehouse, to move the model prediction results to a Power BI visualization.

# CELL ********************

df_pred = df_clean
df_pred['ypred_rfc1_sm'] = ypred_rfc1_sm
table_name = "customer_churn_prediction"
sparkDF=spark.createDataFrame(df_pred)
# save as table new table dbo.customer_churn_prediction using sparkDF.write.mode
sparkDF.write.mode("overwrite").saveAsTable(f"dbo.{table_name}")
print(f"Spark DataFrame saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
