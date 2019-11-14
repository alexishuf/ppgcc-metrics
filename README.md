# ppgcc_metrics

Gather datasets for (self-)evaluation of the Computer Science Graduate program
at Federal University of Santa Catrina (PPGCC/UFSC). Some datasets are specific
to PPGCC (e.g., google calendar and internal spreadsheets).

Some preprocessing and metrics computation is done on the datasets already by
this code, producing more .csv files for further analysis.

## Google API authentication

Some datasets are extracted from Google Sheets and Google Calendar. While such
sheets and calendars are public, API acesss still uses OAuth. Instead of
granting access to human-owned account, you one should create a service account
to perform server-to-server authentication.

You will need a API key and a cryptographic key for server-to-server OAuth
authentication. Google calls this "Service accounts". See the instructions
[here](https://developers.google.com/identity/protocols/OAuth2ServiceAccount). To
put it shortly, go to the [Google Cloud
Console](https://console.developers.google.com/), create a new project, go into
"Credentials" on the sidebar and choose Create credentials > Service
account. Download the JSON file and store it as "service-account-key.json" in
the directory where you will execute the scripts.

On first attempt to use, an error will likely occur stating that the access to
the accessed API (calendar or sheets) is not enabled for the project you created
before. The error message will include an URL where you should add said
permissions to the project. This action has to be done only once.


## Build & Test

The `Makefile` sets up a virtualenv with all required packages and runs
tests. Simply type `make` to do this. To only set up the virtualenv with
dependencies, issue a `make env` command

If you are on an obscure system without make & virtualenv3, read commands under
`test:` in the Makefile and adapt accordingly.

## Interactive shell & Typical usage

This repository targets computer scienntists. Therefore there is no pretty
interfaces. Run the `shell.sh` script to get an IPython shell with instructions
on how to use the python code:

```bash
$ ./shell.sh
.
Python 3.7.4 (default, Oct  4 2019, 06:57:26)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.9.0 -- An enhanced Interactive Python. Type '?' for help.

--=[ ppgcc-metrics interactive shell ]=--
    (actually, just an IPython shell)

Use get_all() to ensure all datasets are available. Use the force=True parameter to force re-download and/or re-processing.
Available datasets:
  ds.CPC_CSV                        at data/cpc.csv                        [PENDING]
  ds.DOCENTES                       at data/docentes.csv
  ds.LINHAS                         at data/linhas.csv
  ds.PPGCC_CALENDAR                 at data/calendar.json                  [PENDING]
  ds.PPGCC_CALENDAR_CSV             at data/calendar.csv                   [PENDING]
  ds.SCHOLAR_CSV                    at data/scholar.csv                    [PENDING]
  ds.SCHOLAR_WORKS_CSV              at data/scholar-works.csv              [PENDING]
  ds.SCOPUS_QUERY                   at data/scopus.qry                     [PENDING]
  ds.SCOPUS_WORKS_CSV               at data/scopus-works.csv               [PENDING]
  ds.SECRETARIA_DISCENTES           at data/secretaria.csv                 [PENDING]
  ds.SUC_DISCENTES_PPGCC            at data/suc-dis-ppgcc.csv              [PENDING]
  de.AUG_DISCENTES                  at data/discentes-augmented.csv        [PENDING]
  de.BIBLIOMETRICS                  at data/bibliometrics-year.csv         [PENDING]
  de.BIBLIOMETRICS_AGGREGATE        at data/bibliometrics.csv              [PENDING]

There are pending datasets. Use their .download() method or the get_all() function.

In [1]: get_all()
Downloading data for cpc.csv...
Downloading data for docentes.csv...
Downloading data for linhas.csv...
Downloading data for calendar.json...
Downloading data for calendar.csv...
Downloading data for scholar.csv...
scholar.csv: Fetching https://scholar.google.com.br/citations?user=a7XTMeIAAAAJ
Fetched 674 documents and 2213 citations for a7XTMeIAAAAJ
scholar.csv: Sleeping 22 seconds
...
```

On a first execution, get_all() will throw an exception blaming the absence of
`scopus-works.csv`. Follow the instructions given by the exception to obtain
said file from the Scopus web interface. Scopus is quite defensive to automated
requests.

# Names comparison (`names.py`)

Names fail miserably as primary keys, nevertheless, they are the primary key in
most of the data sources. Most of the time diferences between correferent names
consists of dropping, adding or abbreviating middle names. Eventually there is a
typo at longer last names.

When sources are moderatedly reliable (bibliometrics and Sucupira), typos are
not forgiven when comparing names. When sources are unreliable (e.g., the
unofficial Google Calendar). Typos are forgiven.

The edit operations allowed when comparing names are the following:

1. All strings have accents removed before comparison
2. All strings have duplicate blank spaces removed before comparison
3. All strings are converted to upper case
4. `L.` == `L` (as a word)
5. `L.` == `Luis` (in first or middle names)
6. Omission/addition of middle name (`Jose L. Silva` == `José L. C. Silva`)
7. Omission/addition of middle name (`Jose L. Silva` == `José L. C. Silva`)
8. Levenshtein distance of 1 for last names with 7 letters or more
9. Levenshtein distance of 2 for last names with 7 letters or more
10. Levenshtein distance of 1 for first and last names with more than 3 letters
11. Levenshtein distance of 2 for first and last names with more than 3 letters

If a match is found using higher-precedence edit operations, lower-precedence
edit operations are not applied. In most cases only up to the 7 first operations
above are applied. When merging sets, ambiguity may prevent matches from being
established. See the code for scenarios where this default is overidden with
`allow_ambiguos=True`

# Datasets metadata

## cpc.csv

Source: PPGCC scientific production reporting sheet, used for professor
accredditaion & external (CAPES) evaluation. All fields but `autores` are
downloaded as-is from the Google Sheets.

| Column name          | Meaning                            |
|----------------------|------------------------------------|
|Prof 1 PPGCC          |                                    |
|Prof 2 PPGCC          |                                    |
|Prof 3 PPGCC          |                                    |
|Prof 4 PPGCC          |                                    |
|Tipo                  | Periódico or Evento                |
|SICLAP                |                                    |
|Ano                   |                                    |
|autores               | extracted from Artigo, same syntax |
|Artigo                |                                    |
|ISSN                  |                                    |
|Sigla                 |                                    |
|Link (DOI)            | HTTP links, which may not be DOIs  |
|Alunos M PPGCC        |                                    |
|Alunos D PPGCC        |                                    |
|Posdocs PPGCC         |                                    |
|Estrangeiros          |                                    |
|Candidato a Lista 4N? |                                    |
|Trabalho Premiado?    |                                    |
|N Profs. (AUTO)       |                                    |
|Pontos (AUTO)         |                                    |

## docentes.csv and linhas.csv

Manually inserted table of professors, including some who already left

| Column       | Meaning|
|--------------|----------------------------------------------|
| docente      | Full name of the professor                   |
| status       | PERMANENTE, COLABROADOR or DESCREDENCIADO    |
| scopus\_id   | Author-ID on Scopus                          |
| scholar\_id  | `user` query param on Scholar                |
| linha        | Main research line                           | 

A professor may be bound to more than one research line. The `linhas.csv` file
lists those associations.

| Column  | Meaning                       |
|---------|-------------------------------|
| docente | Full name, as in docentes.csv |
| linha   | Name of the research line     |

Both files originate from PPGCC
[site](http://ppgcc.posgrad.ufsc.br/linhas-de-pesquisa-2/). `docentes.csv` also
includes additional information from the same source as `cpc.csv`.

## calendar.json and calendar.csv

The JSON file is a dump of all events in the Google Calendar of the
program. This calendar includes all defenses, including qualification exams and
SADs. The dump is obtained using [Google APIs](https://developers.google.com/calendar/v3/reference/events/list).

The `calendar.csv` file contains data extracted from the event summaries and
descriptions, as per the following table:

| Column       | Meaning                                                  |
|--------------|----------------------------------------------------------|
| tipo         | DFM, DFD, EQM, EQD or SAD. Note: DF* means final defense |
| discente     | Full name of the student, as appears in the JSON         |
| orientador   | Adivisor full name, as appears in the JSON               |
| coorientador | Coadvisor full name, if present                          |
| data_ymd     | YYYY-MM-DD date in which the event occured               |

## scholar.csv and scholar-works.csv

Data extracted from the Google Scholar profiles of professors listed in `docentes.csv`. The main csv file contains metrics reported by scholar:

| Column        | Meaning                                                                                                                                                                                                              |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| docente       | Name of the Professor                                                                                                                                                                                                |
| scholar_id    | `user` query parameter                                                                                                                                                                                               |
| documents     | Number of indexed documents by Scholar. Note this may include monographs and duplicates in Scholars own database                                                                                                     |
| citations     | Total number of citations received. This is the sum of the number of citations for all documents authored. If some paper cites two documents authored by this professor, two citations are counted                   |
| docs-citing   | Total number of documents which cite this author. This appears to count only once in the case above. This metric is located in a table of the profile, at the top-right corner and may not be updated as frequently. |
| docs-citing-5 | Same as docs-citing, but restricted to a 5-year sliding window                                                                                                                                                       |
| h-index       | H-index of the professor. This may be updated less frequently than individual citation counts of the documents.                                                                                                      |
| h5-index      | H-index of the professor for documents published in the last 5 years. This may be updated less frequently than individual citation counts of the documents.                                                          |

`scholar-works.csv` contains a deduplicated list of all documents authored by at
least one of the crawled professors. Deduplication removes stop-words and
subtitles from titles, and does a general cleanup (remove accents, duplicate
spaces, convert to upper case) before comparing titles for equality. The author
list is compared using tolerant naming comparison (`names.py`).

## scopus.qry and scopus-works.csv

Scopus is quite defensive against automated requests. To avoid a fragile and
unpredictable crawler, the python code generates a `scopus.qry` file with a
query to be executed on the Scopus web interface. Check all result items and
select **Export**. In the dialog that will appear, select CSV as the format and
check all fields in the Citation and Bibliographical information columns. Save
the resulting CSV file as `data/scopus-works.csv`

The fields are defined by Scopus and are not modified by the python code.

Note that some configurations of Mozilla Firefox may refuse to download the CSV.

## suc-dis-* and suc-dis-ppgcc.csv

These CSVs are pre-cleaned up version from those made available by CAPES for
[2017-2020](https://dadosabertos.capes.gov.br/dataset/2017-2020-discentes-da-pos-graduacao-stricto-sensu-do-brasil)
and
[2013-2016](https://dadosabertos.capes.gov.br/dataset/discentes-da-pos-graduacao-stricto-sensu-do-brasil).
The cleanup consists of:

* Fixing encoding (ISO-8859-1 to UTF-8) 
* Unifying column names to the schema described in the 2017-2020 dataset
  (non-conformant headers in the 2017 dataset are renamed)
* Compressing the CSVs using LZMA during the download
* simplifying `NÃO SE APLICA` to `NA`

See the PDF documentation for the 2017-2020 dataset.

All `csv.xz` files are filterd and merged into a `suc-disc-ppgcc.csv` file that
contains only data pertaining to PPGCC. Every year, Sucupira lists all enrolled
students again. Such duplications are removed so that only the most recent
status for a student at a given level (`DOUTORADO` or `MESTRADO`)
remains. Deduplication is done using sucupira-specific IDs in the CSVs

## secretaria.csv

This is extracted from the [mandatory activities Google
Sheet](https://docs.google.com/spreadsheets/d/1GUhX1Ql3Ky0BzOuIo9CPdKKQg4TIpuW7VYc7MKyl15c/edit#gid=553734804)
into an analysis-friendly CSV file. Many fields receive the exact same name of
their Sucupira counterparts.

| Column                        | Meaning                                                         |
|-------------------------------|-----------------------------------------------------------------|
| NM\_DISCENTE                  | see Sucupira                                                    |
| DS\_GRAU\_ACADEMICO\_DISCENTE | DOUTORADO or MESTRADO                                           |
| DT\_MATRICULA\_DISCENTE       | see Sucupira and `discentes-augmented.csv`                      |
| DT\_SITUACAO\_DISCENTE        | current date, in Sucupira format. See `discentes-augmented.csv` |
| NM\_ORIENTADOR\_PRINCIPAL     | see Sucupira                                                    |
| NM\_COORIENTADOR              | coadvisor name                                                  |
| PRORROG\_1                    | OK if 1st prorrogation aproved                                  |
| PRORROG\_2                    | OK if 1st prorrogation aproved                                  |
| SEM\_TRANCAMENTOS             | Number of semeters with locked enrollment                       |
| DT\_TERMINO                   | deadline for scheduling a defense in Sucupira format            |
| DT\_TERMINO\_ISO              | DT\_TERMINO in YYYY-MM-dd                                       |
| ST\_PROF\_LING\_1             | OK if done                                                      |
| ST\_PROF\_LING\_2             | OK or "não se aplica"                                           |
| ST\_SAD                       | OK or deadline in YYYY/semester                                 |
| ST\_QUALIFICACAO              | OK or deadline in YYYY/semester                                 |
| NUM\_SEMINARIOS               | Number of seminars attended                                     |

## discentes-augmented.csv

This CSV is a merge of `secretaria.csv` and `suc-dis-ppgcc.csv`, with some
additional fields computed from `calendar.json` and `cpc.csv`. All columns of
`secretaria.csv` and `suc-dis-ppgcc.csv` remain (some may be empty) with their
original meanings. The columns below are computed only for this file.

| Column             | Meaning                                                                                                                                                                                                                                                                                                                                                                 |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DT\_MATRICULA\_ISO | same meaning as the sources but in YYYY-MM-DD format                                                                                                                                                                                                                                                                                                                    |
| DT\_SITUACAO\_ISO  | same meaning as the sources but in YYYY-MM-DD format                                                                                                                                                                                                                                                                                                                    |
| N\_CONF            | Number conference papers (co-)authored by the student in the years where he was enrolled under the given level. Only the year is considered, so publication in march counts for a student enrolled in august.                                                                                                                                                           |
| N\_PER             | Same as N\_CONF but for journal articles                                                                                                                                                                                                                                                                                                                                |
| PTS\_CONF          | For each publication in N\_CONF, sum a value between 0 and 1 according to SICLAP assigned at `cpc.csv`. The weights are the same used by CAPES and for the professors accreditation in PPGCC                                                                                                                                                                            |
| PTS\_PER           | Same as PTS\_CONF, but for journal articles                                                                                                                                                                                                                                                                                                                             |
| PTS\_CONF\_IR      | Same as PTS\_CONF, but only considers SICLAP levesls within "índice restrito"                                                                                                                                                                                                                                                                                           |
| PTS\_PER\_IR       | Same as PTS\_PER, but only considers SICLAP levesls within "índice restrito"                                                                                                                                                                                                                                                                                            |
| ST\_REQ\_PUB       | Whether the student achieved his mandatory publication requirements. This considers the rules defined for the level and enrollment semester. Publications found in the calendar event of the student's master defense (if is a PhD student) are disconsidered. Only articles in N_CONF are considered, articles knowingly from before his enrollment are not considered.|

## bibliometrics.csv and bibliometrics-year.csv

Some citation-related metrics collected from `scholar-works.csv` and
`scopus-works.csv`. `bibliometrics.csv` contains these metrics for all
professors and for each indivudual line. Each metric appears once for each
source.

H, H5 and impact factor are computed from the current number of citations of
each document. This may differ from metrics published by scholar since the
preiodicity in which the metrics are computed may vary. However, this approache
allows for computation of the metrics for arbitrary groups of professors.

When computing the metrics for a line, all professores which are listed under
the research line are considered. As a side effect, a single professor may
contribute to the metrics of more than one line. Any authorship position is
considered when selecting the documents for the professors in a
group. Co-authored documents are counted only once, no matter how many
professors of the current group sign it.

| Column      | Meaning                                                                                                                                        |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| group       | Group of professors (all or a research line) considered for the metrics                                                                        | 
| base\_year  | Year of reference for the metric                                                                                                               | 
| source      | Source: `scopus` or `scholar`                                                                                                                  | 
| h           | H-index                                                                                                                                        | 
| h5          | H5-index: considers documents published in the 5 years preceding base\_year, but does not include it                                           |
| documents   | Total number of documents in all time                                                                                                          |
| citations   | Sum of citations of all unique documents                                                                                                       | 
| impact      | Given the N documents published in the two years preceding base_year (but excluding it), this is the sum of their citation counts divided by N |

The file `bibliometrics-year.csv` contains a breakdown fo the same above metrics
(excluding impact) for each year where there is a document for the given group
and source. For `h` and `h5` this breakdown shows all papers published in the
given year whose number of citations is greater or equal to _h_ (or _h5_). For
`documents` it shows the number of unique published documents and citations
shows the number of citations that those documents have today (when the data was
fetched).
