FROM prefecthq/prefect-client:3-latest

# Copy all Python files from the project
COPY *.py /opt/prefect/

WORKDIR /opt/prefect

# Default: keep container running or exit gracefully
# The managed pool will spin up new containers with this image to run flows
CMD ["echo", "Image ready. Deploy locally with: python deploy.py"]
