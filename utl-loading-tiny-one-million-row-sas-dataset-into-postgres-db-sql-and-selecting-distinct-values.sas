%let pgm=utl-loading-tiny-one-million-row-sas-dataset-into-postgres-db-sql-and-selecting-distinct-values;

%sas_submission;

Loading a tiny one million row sas dataset into r postgres db sql database and selecting distint values

   PROCESS

      1 convert sas dataset to postgres table
      2 selet distinct columns using  postgres sql
      3 export result to r dataframe
      4 convert r dataframe to sas dataset

OP was having difficulty with what he/she calls a large table?
Empty SAS Studio Error when using large Work Tables imported from PostgreSQL DB

github
https://tinyurl.com/yc5uvmez
https://github.com/rogerjdeangelis/utl-loading-tiny-one-million-row-sas-dataset-into-postgres-db-sql-and-selecting-distinct-values

stackoverflow sas
https://tinyurl.com/5n7c9vnr
https://stackoverflow.com/questions/79531705/empty-sas-studio-error-when-using-large-work-tables-imported-from-postgresql-db

RELATED REPOS
--------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-partial-key-matching-and-luminosity-in-gene-analysis-sas-r-python-postgresql
https://github.com/rogerjdeangelis/utl-pivot-wide-when-variable-names-contain-values-sql-and-base-r-sas-oython-excel-postgreSQL
https://github.com/rogerjdeangelis/utl-saving-and-creating-r-dataframes-to-and-from-a-postgresql-database-schema

/*
 _ __  _ __ ___ _ __
| `_ \| `__/ _ \ `_ \
| |_) | | |  __/ |_) |
| .__/|_|  \___| .__/
|_|            |_|
*/

THERE IS QUIT A BIT OF PREP WITH POSTGRES.
WHICH IS WHY I LIKE R SQLDF and Python PDSQL

NOTE: POSTGRES EXPECTES LOWER CASE COLUMN NAMES?

1 BUILD DATABASE
================
  %utl_rbeginx;
  parmcards4;
  # Load the necessary libraries
  library(RPostgres)
  library(DBI)
  library(haven)
  a<-read_sas("d:/sd1/a.sas7bdat")
  con <- dbConnect(RPostgres::Postgres(),
              dbname = "postgres",  # Use the default 'postgres' database
              host = "localhost",   # Replace with your PostgreSQL server address if not local
              port = 5432,          # Default PostgreSQL port
              user = "postgres",
              password = "12345678")
  dbExecute(con, "CREATE DATABASE devel")
  dbDisconnect(con)
  ;;;;
  %utl_rendx;

2 BUILD SCHEMA
===============
  %utl_rbeginx;
  parmcards4;
  # Load the necessary libraries
  library(RPostgres)
  library(DBI)
  library(haven)
  a<-read_sas("d:/sd1/a.sas7bdat")
  con <- dbConnect(RPostgres::Postgres(),
              dbname = "devel",
              host = "localhost",
              port = 5432,
              user = "postgres",
              password = "12345678")
  dbExecute(con, "CREATE SCHEMA demographics")
  dbDisconnect(con)
  ;;;;
  %utl_rendx;
/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/
/**************************************************************************************************************************/
/*            INPUT                               |          PROCESS                        |         OUTPUT              */
/*            =====                               |          =======                        |         ======              */
/*                                                |                                         |                             */
/*  SD1.HAVE total obs=1,200,000                  | %utl_rbeginx;                           |   OBS     FRO    UNQ        */
/*                                                | parmcards4;                             |                             */
/*   Variables in Creation Order                  | # Load the necessary libraries          |  COLUMN COL1 DISTINCT       */
/*                                                | library(RPostgres)                      |                             */
/*  #    Variable    Type    Len                  | library(DBI)                            |    1  col1_unq  Alabama     */
/*                                                | library(haven)                          |    2  col1_unq  Alaska      */
/*  1    col1        Char     64                  | library(sqldf)                          |    3  col1_unq  Arizona     */
/*  2    col2        Char     64                  | source("c:/oto/fn_tosas9x.R");          |    4  col1_unq  Arkansas    */
/*  3    col3        Char    256                  | have<-read_sas("d:/sd1/have.sas7bdat")  |    5  col1_unq  California  */
/*                                                | con <- dbConnect(RPostgres::Postgres(), |    6  col1_unq  Colorado    */
/*                                                |   dbname = "devel",                     |   10  col1_unq  Georgia     */
/*  NOTE LOWER CASE NAMES                         |   host = "localhost",                   |  ...                        */
/*                                                |   port = 5432,                          |   45  col1_unq  Vermont     */
/*  col1              col2           col3         |   user = "postgres",                    |   46  col1_unq  Virginia    */
/*                                                |   password = "12345678")                |   47  col1_unq  Washington  */
/*  Louisiana       Georgia        Texas          | dbExecute(con                           |   48  col1_unq  West Virgin */
/*  South Carolina  Maine          Colorado       |  ,"SET search_path TO demographics")    |   49  col1_unq  Wisconsin   */
/*  Florida         Alaska         Utah           | dbWriteTable(con, "have"                |   50  col1_unq  Wyoming     */
/*  Wisconsin       Hawaii         Florida        |   ,have                                 |                             */
/*  Idaho           Nebraska       Missouri       |   ,row.names = FALSE                    |  COLUMN COL2 DISTINCT       */
/*  Utah            Nevada         North Dakota   |   ,overwrite = TRUE)                    |                             */
/*  Texas           Colorado       Ohio           | unqcol1 <- dbGetQuery(con               |   51  col2_unq  Alabama     */
/*  Pennsylvania    Hawaii         Maryland       |  ,paste("SELECT DISTINCT col1 FROM"     |   52  col2_unq  Alaska      */
/*  Louisiana       Nebraska       Wyoming        |  ,"have"))                              |   53  col2_unq  Arizona     */
/*  Georgia         Massachusetts  Pennsylvania   | unqcol2 <- dbGetQuery(con               |   54  col2_unq  Arkansas    */
/*  Kentucky        Hawaii         Minnesota      |   ,paste("SELECT DISTINCT col2 FROM"    |   55  col2_unq  California  */
/*  Oregon          Washington     Minnesota      |   ,"have"))                             |   ,,                        */
/*  South Dakota    Tennessee      Montana        | unqcol3 <- dbGetQuery(con               |   95  col2_unq  Vermont     */
/*  Oregon          Utah           Iowa           |   ,paste("SELECT DISTINCT col3 FROM"    |   96  col2_unq  Virginia    */
/*  Nevada          North Dakota   Mississippi    |   ,"have"))                             |   97  col2_unq  Washington  */
/*  Vermont         Maine          Iowa           | alltre<-sqldf("                         |   98  col2_unq  West Virgin */
/*  Iowa            Colorado       Illinois       |    select                               |   99  col2_unq  Wisconsin   */
/*  Michigan        Delaware       New Mexico     |       'col1_unq' as fro                 |  100  col2_unq  Wyoming     */
/*  Texas           Washington     Oregon         |       ,col1   as unq                    |                             */
/*  North Carolina  Alaska         North Dakota   |    from                                 |  COLUMN COL3 DISTINCT       */
/*  Kentucky        West Virginia  Pennsylvania   |       unqcol1                           |                             */
/*  South Carolina  Iowa           South Carolina |    union                                |  101  col3_unq  Alabama     */
/*  New Hampshire   Maine          Utah           |       all                               |  102  col3_unq  Alaska      */
/*  .....                                         |    select                               |  103  col3_unq  Arizona     */
/*                                                |       'col2_unq' as fro                 |  104  col3_unq  Arkansas    */
/*                                                |       ,col2   as unq                    |  105  col3_unq  California  */
/*  FROM MY AUTOEXEC FILE                         |    from                                 |  106  col3_unq  Colorado    */
/*                                                |       unqcol2                           |  107  col3_unq  Connecticut */
/*  %let stateslnq=%sysfunc(compbl(%str(          |    union                                |                             */
/*   "Alabama"        ,"Alaska"                   |       all                               |  145  col3_unq  Vermont     */
/*  ,"California"     ,"Colorado"                 |    select                               |  146  col3_unq  Virginia    */
/*  ,"Florida"        ,"Georgia"                  |       'col3_unq' as fro                 |  147  col3_unq  Washington  */
/*  ,"Illinois"       ,"Indiana"                  |       ,col3   as unq                    |  148  col3_unq  West Virgin */
/*  ,"Kentucky"       ,"Louisiana"                |    from                                 |  149  col3_unq  Wisconsin   */
/*  ,"Massachusetts"  ,"Michigan"                 |       unqcol3                           |  150  col3_unq  Wyoming     */
/*  ,"Missouri"       ,"Montana"                  |    ")                                   |                             */
/*  ,"New Hampshire"  ,"New Jersey"               | str(alltre)                             |                             */
/*  ,"North Carolina" ,"North Dakota"             | dbListObjects(con                       |                             */
/*  ,"Oregon"         ,"Pennsylvania"             |    ,Id(schema = 'demographics'))        |                             */
/*  ,"South Dakota"   ,"Tennessee"                | dbDisconnect(con)                       |                             */
/*  ,"Vermont"           ,"Virginia"              | df;                                     |                             */
/*  ,"Arizona"        ,"Arkansas"                 | fn_tosas9x(                             |                             */
/*  ,"Connecticut"    ,"Delaware"                 |       inp    = alltre                   |                             */
/*  ,"Hawaii"         ,"Idaho"                    |      ,outlib ="d:/sd1/"                 |                             */
/*  ,"Iowa"           ,"Kansas"                   |      ,outdsn ="want"                    |                             */
/*  ,"Maine"          ,"Maryland"                 |      );                                 |                             */
/*  ,"Minnesota"      ,"Mississippi"              | ');                                     |                             */
/*  ,"Nebraska"       ,"Nevada"                   | ;;;;                                    |                             */
/*  ,"New Mexico"     ,"New York"                 | %utl_rendx;                             |                             */
/*  ,"Ohio"           ,"Oklahoma"                 |                                         |                             */
/*  ,"Rhode Island"   ,"South Carolina"           |                                         |                             */
/*  ,"Texas"          ,"Utah"                     |                                         |                             */
/*  ,"Washington"     ,"West Virginia"            |                                         |                             */
/*  ,"Wisconsin"      ,"Wyoming")));              |                                         |                             */
/*                                                |                                         |                             */
/*                                                |                                         |                             */
/*   options validvarname=v7;                     |                                         |                             */
/*   libname sd1 "d:/sd1";                        |                                         |                             */
/*   data sd1.have(compress=char);                |                                         |                             */
/*      length col1 col2 $64.col3 $256.;          |                                         |                             */
/*      array states[50] $64 (&stateslnq);        |                                         |                             */
/*      do rec=1 to 1200000;                      |                                         |                             */
/*        index=int(50*uniform(54321)) +1;        |                                         |                             */
/*          col1 = states[index];                 |                                         |                             */
/*        index=int(50*uniform(54321)) +1;        |                                         |                             */
/*          col2 = states[index];                 |                                         |                             */
/*        index=int(50*uniform(54321)) +1;        |                                         |                             */
/*          col3 = states[index];                 |                                         |                             */
/*        output;                                 |                                         |                             */
/*      end;                                      |                                         |                             */
/*      keep col:;                                |                                         |                             */
/*   stop;                                        |                                         |                             */
/*   run;quit;                                    |                                         |                             */
/*************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

%let stateslnq=%sysfunc(compbl(%str(
 "Alabama"        ,"Alaska"
,"California"     ,"Colorado"
,"Florida"        ,"Georgia"
,"Illinois"       ,"Indiana"
,"Kentucky"       ,"Louisiana"
,"Massachusetts"  ,"Michigan"
,"Missouri"       ,"Montana"
,"New Hampshire"  ,"New Jersey"
,"North Carolina" ,"North Dakota"
,"Oregon"         ,"Pennsylvania"
,"South Dakota"   ,"Tennessee"
,"Vermont"           ,"Virginia"
,"Arizona"        ,"Arkansas"
,"Connecticut"    ,"Delaware"
,"Hawaii"         ,"Idaho"
,"Iowa"           ,"Kansas"
,"Maine"          ,"Maryland"
,"Minnesota"      ,"Mississippi"
,"Nebraska"       ,"Nevada"
,"New Mexico"     ,"New York"
,"Ohio"           ,"Oklahoma"
,"Rhode Island"   ,"South Carolina"
,"Texas"          ,"Utah"
,"Washington"     ,"West Virginia"
,"Wisconsin"      ,"Wyoming")));


options validvarname=v7;
libname sd1 "d:/sd1";
data sd1.have (compress=char);
   length col1 col2 $64.col3 $256.;
   array states[50] $64 (&stateslnq);
   do rec=1 to 1200000;
     index=int(50*uniform(54321)) +1;
       col1 = states[index];
     index=int(50*uniform(54321)) +1;
       col2 = states[index];
     index=int(50*uniform(54321)) +1;
       col3 = states[index];
     output;
   end;
   keep col:;
stop;
run;quit;

/**************************************************************************************************************************/
/* SD1.HAVE total obs=1,200,000                                                                                           */
/*     Obs    col1              col2              col3                                                                    */
/*                                                                                                                        */
/*       1    North Dakota      Louisiana         Rhode Island                                                            */
/*       2    New York          Oregon            Georgia                                                                 */
/*       3    Kentucky          Alaska            South Carolina                                                          */
/*       4    Wisconsin         Massachusetts     Kentucky                                                                */
/*       5    Michigan          Connecticut       Arizona                                                                 */
/*       6    South Carolina    Delaware          Maryland                                                                */
/*       7    Rhode Island      Georgia           Minnesota                                                               */
/*       8    Nevada            Massachusetts     Pennsylvania                                                            */
/*       9    North Dakota      Connecticut       Wyoming                                                                 */
/*       .....                                                                                                            */
/*                                                                                                                        */
/* 1199994    Texas             Nevada            Alabama                                                                 */
/* 1199995    Nevada            Nevada            New Hampshire                                                           */
/* 1199996    Missouri          South Carolina    Wyoming                                                                 */
/* 1199997    Wyoming           Utah              Alaska                                                                  */
/* 1199998    Louisiana         Rhode Island      Nevada                                                                  */
/* 1199999    California        Virginia          Texas                                                                   */
/* 1200000    Iowa              Washington        Delaware                                                                */
/**************************************************************************************************************************/



proc datasets lib=sd1 nolist nodetails;
 delete alltre;
run;quit;

%utl_rbeginx;
parmcards4;
# Load the necessary libraries
library(RPostgres)
library(DBI)
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R");
have<-read_sas("d:/sd1/have.sas7bdat")
con <- dbConnect(RPostgres::Postgres(),
  dbname = "devel",
  host = "localhost",
  port = 5432,
  user = "postgres",
  password = "12345678")
dbExecute(con
 ,"SET search_path TO demographics")
dbWriteTable(con, "have"
  ,have
  ,row.names = FALSE
  ,overwrite = TRUE)
unqcol1 <- dbGetQuery(con
 ,paste("SELECT DISTINCT col1 FROM"
 ,"have"))
unqcol2 <- dbGetQuery(con
  ,paste("SELECT DISTINCT col2 FROM"
  ,"have"))
unqcol3 <- dbGetQuery(con
  ,paste("SELECT DISTINCT col3 FROM"
  ,"have"))
alltre<-sqldf("
   select
      'col1_unq' as fro
      ,col1   as unq
   from
      unqcol1
   union
      all
   select
      'col2_unq' as fro
      ,col2   as unq
   from
      unqcol2
   union
      all
   select
      'col3_unq' as fro
      ,col3   as unq
   from
      unqcol3
   ")
str(alltre)
alltre
dbListObjects(con
   ,Id(schema = 'demographics'))
dbDisconnect(con)
df;
fn_tosas9x(
      inp    = alltre
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     );
');
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/* R                      SAS                                                                                             */
/*                                                                                                                        */
/* > alltre                                                                                                               */
/*          fro            unq      OBS     FRO    UNQ                                                                    */
/*                                                                                                                        */
/* 1   col1_unq        Alabama       1  col1_unq  Alabama                                                                 */
/* 2   col1_unq         Alaska       2  col1_unq  Alaska                                                                  */
/* 3   col1_unq        Arizona       3  col1_unq  Arizona                                                                 */
/* 4   col1_unq       Arkansas       4  col1_unq  Arkansas                                                                */
/* 5   col1_unq     California       5  col1_unq  California                                                              */
/* 6   col1_unq       Colorado       6  col1_unq  Colorado                                                                */
/* ...                                                                                                                    */
/* 45  col3_unq        Vermont      145  col3_unq  Vermont                                                                */
/* 46  col3_unq       Virginia      146  col3_unq  Virginia                                                               */
/* 47  col3_unq     Washington      147  col3_unq  Washington                                                             */
/* 48  col3_unq  West Virginia      148  col3_unq  West Virgin                                                            */
/* 49  col3_unq      Wisconsin      149  col3_unq  Wisconsin                                                              */
/* 50  col3_unq        Wyoming      150  col3_unq  Wyoming                                                                */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
