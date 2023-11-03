FROM registry.access.redhat.com/ubi9/ubi  AS build_stage

ARG SMDEV_CONTAINER_OFF=1

RUN subscription-manager register --org=$ORG_ID --activationkey=$ACTIVATION_KEY

RUN dnf install -y httpd

RUN dnf repolist

RUN dnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm \
    java-1.8.0-openjdk-headless \
    java-1.8.0-openjdk-devel

ENV R_VERSION=4.2.3

# RUN dnf \
#    -y install \
#    --disableplugin subscription-manager \
#    https://cdn.rstudio.com/r/rhel-9/pkgs/R-${R_VERSION}-1-1.x86_64.rpm

RUN dnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y R-core-devel R-devel

#RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
#RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN dnf clean all


# build out local R stuff
WORKDIR /app
COPY setup.R .
RUN mkdir r-lib-local
RUN R CMD javareconf -e "R -f setup.R"
RUN curl -o SimbaJDBC.zip https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.13.1016.zip
RUN unzip SimbaJDBC.zip -d bq_jdbc
RUN tar czf ohdsi-build.tgz r-lib-local bq_jdbc

# Final VM  
FROM registry.access.redhat.com/ubi9/ubi  AS deploy_stage

ENV SMDEV_CONTAINER_OFF=1

RUN subscription-manager register --org=$ORG_ID --activationkey=$ACTIVATION_KEY

WORKDIR /app

RUN dnf install -y httpd

RUN dnf repolist

RUN dnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm \
    java-1.8.0-openjdk-headless \
    unzip


ENV R_VERSION=4.2.3

#RUN dnf \
#    -y install \
#    --disableplugin subscription-manager \
#    https://cdn.rstudio.com/r/rhel-9/pkgs/R-${R_VERSION}-1-1.x86_64.rpm


RUN dnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y R-core-devel R-devel

#RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
#RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript



ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install gcloud tools
COPY gcloud_setup.sh .
RUN sh gcloud_setup.sh .


COPY --from=build_stage /app /app


