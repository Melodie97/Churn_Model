# Churn Model for Banking Customers
Banks, like any other business, face the risk of losing customers, and because acquiring new clients often costs more than retaining existing ones, they place importance in customer retention.

Churn prediction modelling techniques attempt to understand the precise customer behaviours and attributes that signal the risk and timing of customers leaving.
This helps Marketers & Customer Success experts to predict in advance which customers are going to churn and set up a plan of marketing actions that will have the greatest retention impact on each customer.

### Methodology
The development of the customer churn model involved several stages – data collection, data cleaning & exploratory data analysis (EDA), feature selection & engineering, model building, model optimisation, prediction & Deployment.

#### Data Collection for Training & Prediction
The first step was data collection, which involved writin SQL Scripts to sort and extract data from the Bank's database. The scripts can be found in the **SQL scripts for data collection** folder.

#### Data Cleaning & Exploration
The data was then transferred to a jupyter notebook and cleaned. The cleaning process involved:
- dropping duplicate rows
- experimenting with imputting missing values with constant values and dropping rows with null values
- dropping rows with abnormal values
- changing data types
Exploratory data analysis of the data was then carried out to gain a good understanding of customer patterns and behaviours.

#### Feature Selection & Engineering
This stage involved selecting features of importance to the target variable and engineering new features from the dependent variables. Some features that were engineered include:
- average 5 months customer transaction count
- standard deviation of 5 months customer transaction count

#### Model Training, Evaluation and Selection
Random Forest and XGBoost Classifiers were used to train different data models. Both classifiers also had their hyper parameters tuned to help in accurately classifying the target classes. E.g the scale_weight_pos hyper parameter of the XGB Classifier and the class_weight hyper parameter of the Random Forest Classifier were tuned to handle class imbalance.
The Evaluation metrics used was recall and precision, with more importance given to the recall.

#### Prediction & Deployment
New data was fed into the chosen algorithm for prediction. The model performed a little less on new data, with a recall of 75% on inference
The model was deployed on the bank's internal server as a scheduled script that sends monthly predictions to a database table. Also prediction was visualized on the bank's data platform.