AI-Powered Financial Transaction Categorisation System (GCP)

This repository contains the complete implementation of my GHCI 2025 Hackathon Round-2 solution under the theme
Automated AI-Based Financial Transaction Categorisation.

The system is a fully automated ML pipeline built on Google Cloud Platform, using:

Apache Airflow (Cloud Composer)
BigQuery
Vertex AI embeddings
BigQuery ML DNN classifier
GCS for storage

Custom taxonomy configuration

The solution processes raw noisy transaction strings and predicts categories with high accuracy, meeting and exceeding the hackathon requirement of F1 ≥ 0.90.

End-to-End Pipeline Overview

CSV uploaded to Google Cloud Storage---->Airflow detects the file---->Raw data loaded into BigQuery---->ETL transforms data---->Vertex AI generates embeddings---->BigQuery ML trains a DNN classifier---->Predictions updated with confidence scores---->Reporting view combines historical + predicted categories

Admin updates taxonomy.json without touching code

📁 Repository Structure
transaction-categorization-gcp/
│
├── README.md
├── data/
│   └── noisy_2000_transactions_.csv
├── config/
│   └── taxonomy.json
├── airflow_dag/
│   └── backbase_transactions_categorization_final.py
├── screenshots/
│   ├── architecture_diagram_1.png
│   ├── architecture_diagram_2.png
│   ├── model_metrics.png
│   └── confusion_matrix.png
└── demo/
    └── demo_video_link.txt

Dataset

The dataset contains 2000+ real-world style transactions, including ~50 unlabeled entries used to test the prediction pipeline.

File:
data/noisy_2000_transactions_.csv

Machine Learning Components
✔ Vertex AI text-embedding-004
Converts transaction descriptions into 768-dimensional vectors.

✔ BigQuery ML DNN Classifier
128-64-32 hidden layers
Dropout 0.2
Auto class weighting
Early stopping

✔ Final Output

Each row gets:
predicted_category
confidence_score
final_category (historical or predicted)

Airflow DAG

File:
airflow_dag/backbase_transactions_categorization_final.py

Automates the entire pipeline:

Dataset creation
Raw data load
ETL
Embedding generation
Model training
Prediction
Reporting view creation
Supports idempotency and incremental runs.

🔧 Taxonomy Configuration

Located in:
config/taxonomy.json


The taxonomy is fully admin-editable without code changes.

 Model Performance

The final model achieved:

F1 Score: 0.9454
Precision: 0.9453
Recall: 0.9477
AUC: 0.9665

Screenshots available in screenshots/.

Demo Video
See video demonstration here:
demo/demo_video_link.txt

Conclusion

This system demonstrates a real-world, production-inspired financial transaction categorisation pipeline using GCP, achieving high accuracy, scalability, and customisation.
It addresses the hackathon requirements fully and provides a foundation for real banking deployment.
