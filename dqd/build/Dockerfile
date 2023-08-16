FROM registry.access.redhat.com/rhel7  AS build_stage
ARG ORG_ID
ARG ACTIVATION_KEY
RUN sed -i 's/\(def in_container():\)/\1\n    return False/g' /usr/lib64/python*/*-packages/rhsm/config.py
RUN subscription-manager register --org=$ORG_ID --activationkey=$ACTIVATION_KEY
RUN yum -y update
RUN yum -y groupinstall 'Development Tools'
RUN rm -fr /var/cache/yum/*
RUN yum clean all
RUN subscription-manager repos --enable rhel-7-server-optional-rpms --enable rhel-7-server-extras-rpms 
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install epel-release
#RUN yum -y install pcre2-devel
#RUN yum -y install texinfo-tex
RUN yum -y install harfbuzz-devel fribidi-devel
RUN yum -y install freetype-devel libpng-devel libtiff-devel libjpeg-turbo-devel
# RUN yum -y install R-core R-core-devel libcurl-devel cairo-devel openssl-devel libxml2-devel java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel
RUN yum -y install libcurl-devel cairo-devel openssl-devel libxml2-devel java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel


ENV R_VERSION=4.2.3
RUN curl -O https://cdn.rstudio.com/r/centos-7/pkgs/R-${R_VERSION}-1-1.x86_64.rpm
RUN yum -y install R-$R_VERSION-1-1.x86_64.rpm

RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN yum clean all


# build out local R stuff
WORKDIR /app
COPY setup.R .
RUN mkdir r-lib-local
RUN R CMD javareconf -e "R -f setup.R"   #this install the R packages that are needed
RUN curl -o SimbaJDBC.zip https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.13.1016.zip
RUN unzip SimbaJDBC.zip -d bq_jdbc
RUN tar czf ohdsi-build.tgz r-lib-local bq_jdbc


# # copy to scratch -- NOTE: this might not be needed
# FROM scratch AS artifact
# COPY --from=build_stage /app/ohdsi-build.tgz .

# Final VM  
FROM registry.access.redhat.com/rhel7  AS deploy_stage
ARG ORG_ID
ARG ACTIVATION_KEY
RUN sed -i 's/\(def in_container():\)/\1\n    return False/g' /usr/lib64/python*/*-packages/rhsm/config.py
RUN subscription-manager register --org=$ORG_ID --activationkey=$ACTIVATION_KEY
RUN subscription-manager repos --enable rhel-7-server-optional-rpms --enable rhel-7-server-extras-rpms 
WORKDIR /app
RUN yum -y update
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install epel-release
# RUN yum -y install R-core java-1.8.0-openjdk-headless unzip
RUN yum -y install java-1.8.0-openjdk-headless unzip

ENV R_VERSION=4.2.3
RUN curl -O https://cdn.rstudio.com/r/centos-7/pkgs/R-${R_VERSION}-1-1.x86_64.rpm
RUN yum -y install R-$R_VERSION-1-1.x86_64.rpm


RUN ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R
RUN ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript



ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install gcloud tools
COPY gcloud_setup.sh .
RUN sh gcloud_setup.sh .


COPY --from=build_stage /app /app


