# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1c1d20a7-835f-4905-82ba-a67cc64e28e2",
# META       "default_lakehouse_name": "Travel_Data",
# META       "default_lakehouse_workspace_id": "bb8a6dfd-54da-476a-ab9e-ad88225d6635",
# META       "known_lakehouses": [
# META         {
# META           "id": "1c1d20a7-835f-4905-82ba-a67cc64e28e2"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "21653947-f504-8192-445e-028c1b5a2757",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

flights_df = spark.table("flights")
# preview of flights table
display(flights_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hotels_df = spark.table("hotels")
display(hotels_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

users_df = spark.table("users")
display(users_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to pandas df
flights = flights_df.toPandas()
flights.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hotels = hotels_df.toPandas()
hotels.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

users = users_df.toPandas()
users.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# task
# convert pandas df to appropriate date format for date column

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(flights.shape)
print(hotels.shape)
print(users.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

n1=flights.isnull().sum()
n2=hotels.isnull().sum()
n3=users.isnull().sum()
print(f"Null count in flights: \n{n1}")
print(f"Null count in hotels: \n{n2}")
print(f"Null count in users: \n{n3}")
#There are no null values in the datasets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_flights= flights.select_dtypes(include="number").columns
Q1=flights[num_flights].quantile(0.25)
Q3=flights[num_flights].quantile(0.75)
IQR=Q3-Q1
lower=Q1-1.5*IQR
upper=Q3+1.5*IQR

mask_outlier = (flights[num_flights] < lower) | (flights[num_flights] > upper)
row_has_outlier = mask_outlier.any(axis=1)
outlier_rows = flights[row_has_outlier]
print(f"Rows with outlier: {outlier_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(15, 8))
flights[num_flights].boxplot(rot=45)
plt.title("Outliers in Flight Dataset", fontsize=14)
plt.ylabel("Values")
plt.tight_layout()
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#The travelcode can vary for different range so its not an outlier issue and other features did ot have that much outliers.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_hotels= hotels.select_dtypes(include="number").columns
Q1=hotels[num_hotels].quantile(0.25)
Q3=hotels[num_hotels].quantile(0.75)
IQR=Q3-Q1
lower=Q1-1.5*IQR
upper=Q3+1.5*IQR

mask_outlier = (hotels[num_hotels] < lower) | (hotels[num_hotels] > upper)
row_has_outlier = mask_outlier.any(axis=1)
outlier_rows = hotels[row_has_outlier]
print(f"Rows with outlier: {outlier_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(15, 8))
hotels[num_hotels].boxplot(rot=45)
plt.title("Outliers in Hotel Dataset", fontsize=14)
plt.ylabel("Values")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Again the same as flights

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# Merge flights with users on userCode
flights_users = pd.merge(flights, users, left_on='userCode', right_on='code', how='left')

# Merge the result with hotels on travelCode and userCode
merged_data = pd.merge(flights_users, hotels, on=['travelCode', 'userCode'], how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


merged_data.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data.duplicated().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data.describe().T

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in list(merged_data.columns):
  print(f'{i} : {merged_data[i].nunique()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Unique Values for each variable.
for i in list(merged_data.columns):
    print(f'{i} : {merged_data[i].value_counts()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(10, 6))
sns.histplot(flights['price'], kde=True, bins=30)
plt.title('Distribution of Flight Prices')
plt.xlabel('Price')
plt.ylabel('Frequency')
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(8, 5))
sns.countplot(data=flights, x='flightType')
plt.title('Distribution of Flight Types')
plt.xlabel('Flight Type')
plt.ylabel('Count')
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gender_counts = users['gender'].value_counts()

# Plot the pie chart
plt.figure(figsize=(8, 5))
plt.pie(gender_counts, labels=gender_counts.index, autopct='%1.1f%%', colors=['#66c2a5', '#fc8d62', '#8da0cb'], startangle=140)
plt.title("Gender Distribution of Users")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(8, 6))
plt.boxplot(merged_data['time'], vert=False, patch_artist=True, boxprops=dict(facecolor='lightgreen'))
plt.xlabel('Flight Time')
plt.title('Distribution of Flight Times')
plt.savefig('chart2.png')
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(10, 5))
users['company'].value_counts().plot(kind='bar', color='skyblue')
plt.title("Distribution of Users by Company")
plt.xlabel("Company")
plt.ylabel("User Count")
plt.xticks(rotation=45)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.histplot(merged_data["total"], bins=30, kde=True)
plt.title("Total Cost Distribution")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.scatterplot(data=merged_data, x="distance", y="time", hue="flightType")
plt.title("Distance vs Travel Time")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

route_price = (
    merged_data.groupby(["from", "to"])["price_x"]
      .mean()
      .reset_index()
      .sort_values("price_x", ascending=False)
      .head(10)
)

sns.barplot(data=route_price, x="price_x", y="to")
plt.title("Top Expensive Routes")
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data["date_x"] = pd.to_datetime(merged_data["date_x"])

daily = merged_data.groupby(merged_data["date_x"].dt.date).size()

daily.plot(kind="line")
plt.title("Trips Over Time")
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_cols = ["price_x", "price_y", "distance", "time", "days", "total"]

sns.heatmap(merged_data[num_cols].corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.countplot(data=merged_data, y="agency", order=merged_data["agency"].value_counts().index)
plt.title("Trips by Agency")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.pairplot(merged_data[num_cols], diag_kind='kde')
plt.suptitle('Pair Plot of Numerical Features', y=1.02)  # Add a title
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Encoding flight type
flights['flightType'] = flights['flightType'].replace({'economic': 0, 'premium': 1, 'firstClass':3})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converting categorical variables to dummy/indicator variables
flights = pd.get_dummies(flights, columns=['from', 'to', 'flightType', 'agency'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Selecting features for the model 
X = flights.drop(columns=['travelCode', 'userCode', 'price', 'date'])
y = flights['price']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Scaling Features
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()

X_train = scaler.fit_transform(X_train)   # fit on train
X_test  = scaler.transform(X_test)        # ONLY transform test


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Model Implementation Using ML Flow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

X_train.shape, X_test.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor

mlflow.set_experiment("flight-price-regression")

model = RandomForestRegressor(
    n_estimators=200,
    max_depth=15,
    min_samples_leaf=10,
    random_state=42
)


with mlflow.start_run():
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mlflow.log_param("model", "RandomForestRegressor")
    mlflow.log_param("n_estimators", 300)

    mlflow.log_metric("MAE", mean_absolute_error(y_test, y_pred))
    mlflow.log_metric("RMSE", np.sqrt(mean_squared_error(y_test, y_pred)))
    mlflow.log_metric("R2", r2_score(y_test, y_pred))

    mlflow.sklearn.log_model(model, "flight_price_model")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

mae, rmse, r2


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.end_run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.active_run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt

plt.scatter(y_test, y_pred, alpha=0.3)
plt.xlabel("Actual Price")
plt.ylabel("Predicted Price")
plt.title("Actual vs Predicted Flight Prices")
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Nearly straight Graph means the Model is good.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

residuals = y_test - y_pred

plt.hist(residuals, bins=50)
plt.title("Residual Distribution")
plt.xlabel("Error")
plt.ylabel("Frequency")
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Random Forest Regressor was selected as the final model based on its superior performance. 
#MLflow was used to track the experiment, log evaluation metrics, and store the trained model. 
#The experiment run was explicitly ended to ensure proper lifecycle management.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.search_runs()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow.sklearn

run_id = "6378898b-0edc-4c2e-ad7c-9dc94e889d76"
model_uri = f"runs:/{run_id}/flight_price_model"

model = mlflow.sklearn.load_model(model_uri)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#sample prediction 
sample = X_test[0].reshape(1, -1)
prediction = model.predict(sample)
prediction


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#mlflow done saved the best model as flight_price_model in mlflow log with the above runid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
