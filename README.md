# ppgcc_metrics

Gather datasets for (self-)evaluation of brazilian graduate programs. Some datasets are specific to the Computer Science Graduate program of PPGCC/UFSC.

Some preprocessing and metrics computation is done on the datasets already by this code, producing more .csv files for further analysis.

## Google API authentication

Some datasets are extracted from Google Sheets and Google Calendar. While such sheets and calendars are public, API acesss still uses OAuth. Instead of granting access to human-owned account, you one should create a service account to perform server-to-server authentication. 

You will need a API key and a cryptographic key for server-to-server OAuth authentication. Google calls this "Service accounts". See the instructions [here](https://developers.google.com/identity/protocols/OAuth2ServiceAccount). To put it shortly, go to the [Google Cloud Console](https://console.developers.google.com/), create a new project, go into "Credentials" on the sidebar and choose Create credentials > Service account. Download the JSON file and store it as "service-account-key.json" in the directory where you will execute the scripts.

On first attempt to use, an error will likely occur stating that the access to the accessed API (calendar or sheets) is not enabled for the project you created before. The error message will include an URL where you should add said permissions to the project. This action has to be done only once.


