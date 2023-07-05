# DQD Build Stage

This stage is responsible for building an R-containing RedHat environment in a Docker container. This includes the compilation of R packages. 

## Prerequisites

1. [A RedHat account](https://www.redhat.com/en) must be created to obtain an activation key for the RedHat instance. Follow instructions, follow [Registry Authentication](https://access.redhat.com/RegistryAuthentication).
 
## Contents

**Dockerfile** - The Dockerfile used to run the build script. It installs R, R packages, and the gcloud client.

**setup.R** - Installs R packages needed for DQD.

**gcloud_setup.sh** - Installs the Google Cloud [client library](https://cloud.google.com/sdk/docs/install#rpm).

## Execution

1. Obtain the RedHat OrgID and ActivationKey to be used for the instance.
2. Execute the following command to build the Docker container, replacing the necessary arguments:
```
docker build . -t dq_env --build-arg ORG_ID=my_org_id --build-arg ACTIVATION_KEY=my_activation_key
```
3. (Note) Sometimes the build will silently fail due to failed installation of R packages, which sometimes does not throw a terminating error message. Proof of successfuly comletion should be looked for in the logs.
