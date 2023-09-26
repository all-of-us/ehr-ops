FROM registry.redhat.io/ubi8  AS build_stage

RUN dnf \
    --enablerepo ubi-8-appstream-rpms \
    --enablerepo ubi-8-baseos-rpms \
    --enablerepo ubi-8-codeready-builder-rpms \
    --disableplugin subscription-manager \
    install -y \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm \
    harfbuzz \
    fribidi \
    freetype-devel \
    libpng-devel \
    libtiff-devel \
    libjpeg-turbo-devel \
    libcurl-devel \
    cairo \
    openssl-devel \
    libxml2-devel \
    fontconfig-devel  \
    java-1.8.0-openjdk-headless \
    java-1.8.0-openjdk-devel 



ENV R_VERSION=4.3.1

RUN dnf \
    -y install \
    --disableplugin subscription-manager \
    https://cdn.rstudio.com/r/centos-8/pkgs/R-${R_VERSION}-1-1.x86_64.rpm

RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN dnf clean all


# build out local R stuff
WORKDIR /app
COPY setup.R .
RUN mkdir r-lib-local
RUN R CMD javareconf -e "R -f setup.R"   #this install the R packages that are needed
RUN curl -o SimbaJDBC.zip https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.13.1016.zip
RUN unzip SimbaJDBC.zip -d bq_jdbc
RUN tar czf ohdsi-build.tgz r-lib-local bq_jdbc

# Final VM  
FROM registry.redhat.io/ubi8/ubi  AS deploy_stage

WORKDIR /app

RUN dnf \
    --enablerepo ubi-8-appstream-rpms \
    --enablerepo ubi-8-baseos-rpms \
    --enablerepo ubi-8-codeready-builder-rpms \
    --disableplugin subscription-manager \
    install -y \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm \
    java-1.8.0-openjdk-headless \
    unzip


ENV R_VERSION=4.2.3

RUN dnf \
    # --enablerepo codeready-builder-for-rhel-8-x86_64-rpms \
    -y install \
    --disableplugin subscription-manager \
    https://cdn.rstudio.com/r/centos-8/pkgs/R-${R_VERSION}-1-1.x86_64.rpm


RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript



ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install gcloud tools
COPY gcloud_setup.sh .
RUN sh gcloud_setup.sh .


COPY --from=build_stage /app /app


