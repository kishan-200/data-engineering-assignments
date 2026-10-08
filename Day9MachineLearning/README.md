# Machine Learning Engineering – Hands-On Lab Series 

A practical machine learning project covering regression, classification, tree-based models, ensemble learning, and customer segmentation using Python and Scikit-Learn.

## Overview

This project contains five hands-on machine learning exercises focused on building, evaluating, and interpreting machine learning models using real-world datasets.

The project covers:

- Data preprocessing and feature engineering
- Regression and classification
- Model evaluation and diagnostics
- Decision tree pruning
- Random Forest and hyperparameter tuning
- Feature importance analysis
- K-Means clustering
- Customer segmentation

## Exercises

### 1. Multiple Linear Regression
**Dataset:** Medical Insurance

Predict medical insurance charges using demographic and lifestyle-related features.

**Techniques:**
- Linear Regression
- Feature scaling
- One-hot encoding
- Target transformation
- Residual analysis
- R² and RMSE evaluation

### 2. Logistic Regression
**Dataset:** Bank Marketing

Predict whether a customer will subscribe to a term deposit.

**Techniques:**
- Logistic Regression
- Class-weighted learning
- Feature scaling
- Categorical encoding
- ROC-AUC
- Precision, Recall and F1-score
- Threshold calibration

### 3. Decision Trees
**Dataset:** Bank Marketing

Build and evaluate Decision Tree classifiers while analyzing overfitting and pruning.

**Techniques:**
- Gini impurity
- Entropy
- Tree depth control
- Cost-complexity pruning
- Feature importance
- Decision rule extraction

### 4. Random Forest
**Dataset:** Bank Marketing

Develop an ensemble classification model and compare its performance with other classifiers.

**Techniques:**
- Random Forest
- Out-of-Bag evaluation
- GridSearchCV
- Hyperparameter tuning
- Mean Decrease in Impurity
- Permutation importance
- Model comparison

### 5. K-Means Customer Segmentation
**Dataset:** Wholesale Customers

Segment customers based on their spending patterns.

**Techniques:**
- Feature scaling
- Elbow Method
- Silhouette Score
- K-Means clustering
- Cluster profiling
- Customer persona analysis

## Technologies Used

- Python
- Pandas
- NumPy
- Scikit-Learn
- Matplotlib
- Seaborn
- Google Colab

## Project Structure

```text
Day9MachineLearning/
│
├── README.md
│
├── input/
│   ├── insurance.csv
│   ├── bank-full.csv
│   └── Wholesale customers data.csv
│
├── ML_Engineering_Lab_Series.ipynb
│
└── output/
    └── screenshots/
