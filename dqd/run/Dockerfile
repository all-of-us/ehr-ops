FROM dq_env
ARG SERVICE_ACCT_KEY

WORKDIR /app

COPY dqd_execute.R .
COPY config.yml .
RUN echo $SERVICE_ACCT_KEY > gcp_key.json
# COPY $SERVICE_ACCT_KEY_PATH ./gcp_key.json
RUN export $(xargs < .env)


# RUN Rscript dqd_execute.R
RUN R CMD javareconf -e "R -f dqd_execute.R"