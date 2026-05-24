🚀 Projects Overview
1. NASA APOD ETL Pipeline
Extracted real-time APOD metadata using NASA’s Astronomy Picture of the Day API.
Automatically downloaded and stored daily astronomy images.
Transformed raw JSON responses into clean, structured datasets for analysis.
Loaded processed data into a database with a well-organized ETL folder hierarchy:
raw → images → staged

2. Weather API ETL Pipeline
Extracted real-time weather data from a Weather API.
Collected live atmospheric conditions including:
Temperature
Humidity
Pressure
Wind Speed
Transformed raw API responses into analysis-ready datasets.
Loaded processed data into a database using a structured ETL workflow:
raw → processed → staged

4. Machine Learning Classification Pipeline (Iris & Titanic Datasets)
Extracted and preprocessed structured datasets.
Performed:
Data Cleaning
Feature Engineering
Categorical Encoding
Exploratory Data Analysis (EDA)
Built and evaluated machine learning classification models for:
Iris Flower Species Prediction
Titanic Survival Prediction
Applied performance evaluation metrics to analyze model accuracy and effectiveness.


🛠️ Technologies Used
Python
Pandas
NumPy
Scikit-learn
Requests
SQL / SQLite
Matplotlib / Seaborn
Jupyter Notebook

📊 Key Features
Real-time API data extraction
Automated ETL workflows
Data preprocessing and transformation
Machine Learning model training & evaluation
Organized folder hierarchy for scalable pipelines
