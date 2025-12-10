gcloud config set project ba882-team4-474802

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-team4-474802/streamlit-poc .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-team4-474802/streamlit-poc

echo "======================================================"
echo "deploy run"
echo "======================================================"

gcloud run deploy streamlit-poc \
    --image gcr.io/ba882-team4-474802/streamlit-poc \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account github-ingestor@ba882-team4-474802.iam.gserviceaccount.com \
    --memory 1Gi