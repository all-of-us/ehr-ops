FROM dq_env
ARG SERVICE_ACCT_KEY

WORKDIR /app

COPY dqd_execute.R .
COPY config.yml .
COPY results_upload.sh .

RUN echo $SERVICE_ACCT_KEY > gcp_key.json

# Execute DQD
RUN R CMD javareconf -e "R -f dqd_execute.R"

# Push file to cloud
RUN sh results_upload.sh
