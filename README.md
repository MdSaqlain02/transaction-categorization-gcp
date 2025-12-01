title: "AI-Powered Financial Transaction Categorisation System (GCP)"
badges:
  - "Google Cloud: Data Engineering"
  - "BigQuery: ML Pipelines"
  - "Apache Airflow: Orchestration"
  - "Vertex AI: Embeddings"

description: |
  This repository contains the complete implementation “Automated AI-Based Financial Transaction Categorisation.”
  
  It is a full, production-inspired ML pipeline built on Google Cloud Platform (GCP),
  using Airflow, BigQuery, Vertex AI embeddings, BigQuery ML, and GCS. The system 
  processes noisy transaction descriptions and achieves an F1 score of 0.9454.

architecture_image: "./screenshots/architecture_diagram_1.png"

pipeline_workflow: |
  CSV uploaded to GCS
          ↓
  Airflow detects new file
          ↓
  Raw data ingested into BigQuery
          ↓
  ETL transforms & cleans data
          ↓
  Vertex AI creates 768-d embeddings
          ↓
  BigQuery ML trains DNN classifier
          ↓
  Predictions + confidence generated
          ↓
  Reporting view merges categories
          ↓
  Admin updates taxonomy.json without code changes

tech_stack:
  gcp_services:
    - BigQuery
    - Cloud Storage
    - Cloud Composer (Airflow)
    - Vertex AI (text-embedding-004)
    - IAM
  machine_learning:
    - BigQuery ML DNN Classifier
    - Vertex AI Embeddings
    - Confidence Scoring Pipeline
  tools_languages:
    - Python
    - SQL
    - Git
    - Jupyter Notebook

repository_structure: |
  transaction-categorization-gcp/
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

dataset:
  path: "data/noisy_2000_transactions_.csv"
  details:
    - "2000+ noisy real-world transactions"
    - "Mixed-case, typos, abbreviations"
    - "~50 unlabeled rows for prediction testing"

machine_learning_components:
  vertex_ai_embedding_model: "text-embedding-004"
  embedding_dimension: 768
  bigquery_ml_dnn_classifier:
    architecture:
      - 128
      - 64
      - 32
    dropout: 0.2
    auto_class_weighting: true
    early_stopping: true
  output_fields:
    - predicted_category
    - confidence_score
    - final_category

airflow_dag:
  file_path: "airflow_dag/backbase_transactions_categorization_final.py"
  workflow_steps:
    - Dataset & table creation
    - CSV ingestion to BigQuery
    - ETL transformation
    - Vertex AI embedding generation
    - BigQuery ML training
    - Batch predictions
    - Confidence scoring
    - Reporting view creation
  features:
    - Idempotent
    - Incremental
    - Production-ready
    - Taxonomy-driven

taxonomy_configuration:
  path: "config/taxonomy.json"
  purpose: "Admin-editable merchant-category mapping (no code changes required)."


model_performance:
  f1_score: 0.9454
  precision: 0.9453
  recall: 0.9477
  auc: 0.9665
  screenshots:
    - "screenshots/model_metrics.png"
    - "screenshots/confusion_matrix.png"

demo:
  link_path: "demo/demo_video_link.txt"

architecture_diagrams:
  - "screenshots/architecture_diagram_1.png"
  - "screenshots/architecture_diagram_2.png"

conclusion: |
  This project delivers a complete, scalable, production-grade financial transaction
  categorisation pipeline on GCP. It integrates ETL, ML embeddings, DNN classification,
  orchestration via Airflow, and taxonomy-driven customisation. l
contact:
  name: "Mohammed Saqlain"
  github: "https://github.com/MdSaqlain02"
  linkedin: "https://www.linkedin.com/in/mohammedsaqlain-dev"
  email: "saqlainmohammed005@gmail.com"

quote: "Always learning, always building."
