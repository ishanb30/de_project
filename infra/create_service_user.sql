/*
Assumptions:
    - SPOTIFY_PIPELINE_ROLE and its grants already exist (permissions.sql has run)
    - This file is run with role ACCOUNTADMIN/SYSADMIN/SECURITYADMIN
    - One-time, manual provisioning — not part of automated setup
    - RSA_PUBLIC_KEY below is a placeholder; substitute the real key only when
      running this in a worksheet, and never commit the real value back into this file
*/

CREATE USER IF NOT EXISTS SPOTIFY_PIPELINE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = SPOTIFY_PIPELINE_ROLE
  RSA_PUBLIC_KEY = '<paste your generated public key here>';

GRANT ROLE SPOTIFY_PIPELINE_ROLE TO USER SPOTIFY_PIPELINE_SVC;
