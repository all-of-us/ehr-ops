FROM dq_env
# ARG SERVICE_ACCT_KEY

WORKDIR /app

COPY dqd_execute.R .
COPY config.yml .
COPY results_upload.sh .
COPY run.sh .

# RUN echo $SERVICE_ACCT_KEY > gcp_key.json

# Execute DQD and push file to cloud
RUN ["chmod", "+x", "run.sh"]
ENTRYPOINT [ "sh", "run.sh" ]
