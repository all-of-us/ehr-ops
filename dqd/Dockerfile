FROM registry.redhat.io/rhel7  AS build_stage
ARG USER
ARG PASS
RUN sed -i 's/\(def in_container():\)/\1\n    return False/g' /usr/lib64/python*/*-packages/rhsm/config.py
RUN subscription-manager register --username=$USER --password=$PASS --auto-attach
RUN yum -y update
RUN yum -y groupinstall 'Development Tools'
RUN rm -fr /var/cache/yum/*
RUN yum clean all
RUN subscription-manager repos --enable rhel-7-server-optional-rpms --enable rhel-7-server-extras-rpms 
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install epel-release
#RUN yum -y install pcre2-devel
#RUN yum -y install texinfo-tex
RUN yum -y install R-core R-core-devel libcurl-devel cairo-devel openssl-devel libxml2-devel java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel
RUN yum clean all


# build out local R stuff
WORKDIR /app
COPY setup.R .
RUN mkdir r-lib-local
RUN R CMD javareconf -e "R -f setup.R"   #this install the R packages that are needed
RUN curl -o SimbaJDBC.zip https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.13.1016.zip
RUN unzip SimbaJDBC.zip -d bq_jdbc
RUN tar czf ohdsi-build.tgz r-lib-local bq_jdbc

# copy to scratch -- NOTE: this might not be needed
FROM scratch AS artifact
COPY --from=build_stage /app/ohdsi-build.tgz .

# Final VM  
FROM registry.redhat.io/rhel7  AS deploy_stage
ARG USER
ARG PASS
RUN sed -i 's/\(def in_container():\)/\1\n    return False/g' /usr/lib64/python*/*-packages/rhsm/config.py
RUN subscription-manager register --username=$USER --password=$PASS --auto-attach
WORKDIR /app
RUN yum -y update
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install epel-release
RUN yum -y install R-core java-1.8.0-openjdk-headless unzip
#
COPY --from=build_stage /app /app
