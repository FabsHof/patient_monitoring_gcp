"""
Vertex AI pipeline: Septic Shock Risk prediction.

Pipeline steps:
    1. Ingest   — read patient vitals from BigQuery, derive septic_risk label
    2. Train    — fit a dummy sklearn LogisticRegression
    3. Register — upload model artifact to Vertex AI Model Registry
    4. Deploy   — serve the model on a Vertex AI Endpoint

This script is a dry-run definition.  Running it compiles the pipeline
to YAML and prints a summary — it does NOT submit to Vertex AI.
"""

from kfp import dsl, compiler
from kfp.dsl import Dataset, Model, Input, Output
from google.cloud import aiplatform

from dotenv import load_dotenv, dotenv_values

load_dotenv()

env = dotenv_values()
PROJECT_ID = env.get('PROJECT_ID', 'patient-monitoring-dev')
LOCATION = env.get('VERTEX_LOCATION', 'europe-west4')
BQ_DATASET = env.get('BQ_DATASET', 'patient_monitoring')
BQ_TABLE = env.get('BQ_TABLE', 'patient_vitals')
PIPELINE_ROOT = env.get('PIPELINE_ROOT', f'gs://{PROJECT_ID}-pipeline-artifacts')

SERVING_IMAGE = 'us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest'


# ---------------------------------------------------------------------------
# Pipeline components
# ---------------------------------------------------------------------------

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-bigquery', 'pandas', 'db-dtypes'],
)
def ingest_from_bigquery(
    project_id: str,
    bq_dataset: str,
    bq_table: str,
    output_dataset: Output[Dataset],
):
    """Read vitals from BigQuery and derive a binary septic_risk label."""
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project_id)

    # According to SIRS criteria (https://en.wikipedia.org/wiki/Septic_shock), 
    # septic shock risk is high if:
    # - body temperature > 38°C OR < 36°C
    # - heart rate > 90 bpm
    query = f"""
        SELECT
            heart_rate,
            body_temperature,
            spO2,
            battery_level,
            CASE
                WHEN (body_temperature > 38 OR body_temperature < 36) AND heart_rate > 90 THEN 1
                ELSE 0
            END AS septic_risk
        FROM `{project_id}.{bq_dataset}.{bq_table}`
    """

    df = client.query(query).result().to_dataframe()
    df.to_csv(output_dataset.path, index=False)


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['scikit-learn', 'pandas', 'joblib'],
)
def train_model(
    input_dataset: Input[Dataset],
    output_model: Output[Model],
):
    """Train a logistic-regression model on the ingested vitals data."""
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    import joblib

    df = pd.read_csv(input_dataset.path)
    X = df.drop(columns=['septic_risk'])
    y = df['septic_risk']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
    )

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    print(f'Test accuracy: {score:.4f}')

    joblib.dump(model, output_model.path)


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-aiplatform'],
)
def register_model(
    input_model: Input[Model],
    project_id: str,
    location: str,
    model_display_name: str,
    serving_container_image_uri: str,
) -> str:
    """Upload the trained model to the Vertex AI Model Registry."""
    from google.cloud import aiplatform
    import os

    aiplatform.init(project=project_id, location=location)

    # Copy model artifact to GCS (required by Model.upload)
    gcs_model_dir = f'gs://{project_id}-model-artifacts/{model_display_name}'
    os.system(f'gsutil cp {input_model.path} {gcs_model_dir}/model.joblib')

    model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=gcs_model_dir,
        serving_container_image_uri=serving_container_image_uri,
    )

    print(f'Registered model: {model.resource_name}')
    return model.resource_name


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-aiplatform'],
)
def deploy_model(
    model_resource_name: str,
    project_id: str,
    location: str,
    endpoint_display_name: str,
) -> str:
    """Deploy a registered model to a Vertex AI Endpoint."""
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=location)

    model = aiplatform.Model(model_name=model_resource_name)

    # Reuse existing endpoint or create a new one
    endpoints = aiplatform.Endpoint.list(
        filter=f'display_name="{endpoint_display_name}"',
    )
    endpoint = endpoints[0] if endpoints else aiplatform.Endpoint.create(
        display_name=endpoint_display_name,
    )

    model.deploy(
        endpoint=endpoint,
        machine_type='n1-standard-4',
        min_replica_count=1,
        max_replica_count=1,
    )

    print(f'Deployed to endpoint: {endpoint.resource_name}')
    return endpoint.resource_name


# ---------------------------------------------------------------------------
# Pipeline definition
# ---------------------------------------------------------------------------

@dsl.pipeline(
    name='septic-shock-risk-pipeline',
    description='Ingest from BQ ==> Train ==> Register ==> Deploy',
    pipeline_root=PIPELINE_ROOT,
)
def septic_risk_pipeline(
    project_id: str = PROJECT_ID,
    location: str = LOCATION,
    bq_dataset: str = BQ_DATASET,
    bq_table: str = BQ_TABLE,
    model_display_name: str = 'septic-risk-model',
    endpoint_display_name: str = 'septic-risk-endpoint',
    serving_container_image_uri: str = SERVING_IMAGE,
):
    # Step 1 — Ingest vitals + derive label
    ingest_task = ingest_from_bigquery(
        project_id=project_id,
        bq_dataset=bq_dataset,
        bq_table=bq_table,
    )

    # Step 2 — Train dummy sklearn model
    train_task = train_model(
        input_dataset=ingest_task.outputs['output_dataset'],
    )

    # Step 3 — Register in Model Registry
    register_task = register_model(
        input_model=train_task.outputs['output_model'],
        project_id=project_id,
        location=location,
        model_display_name=model_display_name,
        serving_container_image_uri=serving_container_image_uri,
    )

    # Step 4 — Deploy to endpoint
    deploy_model(
        model_resource_name=register_task.output,
        project_id=project_id,
        location=location,
        endpoint_display_name=endpoint_display_name,
    )


# ---------------------------------------------------------------------------
# Compile (dry run)
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    output_path = 'septic_risk_pipeline.yaml'

    print('1) Compiling pipeline ...')
    compiler.Compiler().compile(
        pipeline_func=septic_risk_pipeline,
        package_path=output_path,
    )
    print(f'   Pipeline compiled ==> {output_path}')

    try:
        print('2) Creating PipelineJob (dry run — not submitted) ...')
        job = aiplatform.PipelineJob(
            display_name='septic-risk-pipeline-run',
            template_path=output_path,
            pipeline_root=PIPELINE_ROOT,
            parameter_values={
                'project_id': PROJECT_ID,
                'location': LOCATION,
                'bq_dataset': BQ_DATASET,
                'bq_table': BQ_TABLE,
            },
        )
        print(f'   PipelineJob ready: {job.display_name}')
        print(f'   To submit: job.run(service_account="<SA_EMAIL>")')

    except Exception as e:
        print(f'⚠️ Skipped PipelineJob creation — no GCP credentials:\n\n{e}\n\n')
        print('Hint: To authenticate, run: gcloud auth application-default login')
