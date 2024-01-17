FROM registry.access.redhat.com/ubi9/ubi-minimal  AS build_stage

ENV SMDEV_CONTAINER_OFF=1
ARG ACTIVATION_KEY
ARG ORG_ID

RUN microdnf install -y subscription-manager

RUN subscription-manager register --org=${ORG_ID} --activationkey=${ACTIVATION_KEY}

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    httpd \
    dirmngr \
    gnupg \
    tar \
    glibc-devel \
    make \
    gzip \
    libtool \
    diffutils \
    findutils \
    procps-ng \
    shadow-utils \
    autoconf \
    automake

RUN microdnf repolist

RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    java-11-openjdk-headless \
    java-11-openjdk-devel

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    openssl-devel libcurl-devel harfbuzz-devel fribidi-devel \
    freetype-devel libpng-devel libtiff-devel libjpeg-devel


RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y R-core-devel R-devel R-java-devel libRmath-devel

ENV PATH="$PATH:$JAVA_HOME/bin"

RUN microdnf clean all

# build out local R stuff
WORKDIR /app
COPY setup.R .
RUN mkdir r-lib-local
RUN R CMD javareconf -e "R -f setup.R"
RUN curl -o SimbaJDBC.zip https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.25.1029.zip
RUN unzip SimbaJDBC.zip -d bq_jdbc
RUN tar czf ohdsi-build.tgz r-lib-local bq_jdbc

# # Final VM  
FROM registry.access.redhat.com/ubi9/ubi-minimal  AS deploy_stage

ENV SMDEV_CONTAINER_OFF=1
ARG ACTIVATION_KEY
ARG ORG_ID

RUN microdnf install -y subscription-manager

RUN subscription-manager register --org=${ORG_ID} --activationkey=${ACTIVATION_KEY}

WORKDIR /app

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    httpd \
    dirmngr \
    gnupg \
    tar \
    glibc-devel \
    make \
    gzip \
    libtool \
    diffutils \
    findutils \
    procps-ng \
    shadow-utils \
    autoconf \
    automake

RUN microdnf repolist

RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    java-11-openjdk-headless \
    unzip

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y \
    openssl-devel libcurl-devel harfbuzz-devel fribidi-devel \
    freetype-devel libpng-devel libtiff-devel libjpeg-devel

RUN microdnf \
    --enablerepo ubi-9-appstream-rpms \
    --enablerepo ubi-9-baseos-rpms \
    --enablerepo ubi-9-codeready-builder-rpms \
    --enablerepo rhel-9-for-x86_64-appstream-rpms \
    --enablerepo codeready-builder-for-rhel-9-x86_64-rpms \
    install -y R-core-devel R-devel R-java-devel libRmath-devel

# Install gcloud tools
COPY gcloud_setup.sh .
RUN sh gcloud_setup.sh .

COPY --from=build_stage /app /app

