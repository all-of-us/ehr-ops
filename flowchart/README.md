```mermaid
flowchart LR
    subgraph Enrollment
        direction TB
        A[Participant] --> |<u><a href='https://joinallofus.atlassian.net/wiki/spaces/DRC/pages/2883800/HealthPro+Release+Notes#HealthPro-Release-1.6.4%2D%2D%2Drelease-date-5%2F31%2F2019'>Finished EHR Consent \n or DV EHR Intent to Share</a></u>| B[Fully Consented]
        B[Fully Consented] --> |Finished 3 Baseline <u><a href=''>PPI Modules</a></u> \n & Biospecimen Collection| C["Core Participant Minus PM (Physical Measurement)"]
        C["Core Participant Minus PM (Physical Measurement)"] --> |Finished PM| D[Core Participant]
    end
    subgraph EHRSubmission
        direction TB
        H[Eligible for EHR Submission] --> |<u><a href='https://aou-ehr-ops.zendesk.com/hc/en-us/articles/6439235968531-SOP-for-Patient-Status-Flag-'>Patient Status Submitted</a></u>| I[Patients at HPO]
        H[Eligible for EHR Submission] --> J[Direct Volunteers]
        I[Patients at HPO] --> |<u><a href='https://aou-ehr-ops.zendesk.com/hc/en-us/articles/6496636754323-2022-05-26-Release-Notes-Revamped-NIH-Grant-Award-Metric-Dashboard'>Data Transfer Rate C</a></u>| K[EHR Received at DRC]
    end
    subgraph Retention
        direction TB
        E[Eligible for Retention] --> F[Active Retention]
        F[Active Retention] --> G[Passive Retention]
    end
    Enrollment --> EHRSubmission --> Retention
```
