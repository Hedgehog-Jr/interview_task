# Task 1

## In order to run app, you need to execute entrypoint.sh script. Docker is required
Example of proceeded data is in the `results` folder

Place XML files in `input` folder.
Result file will be placed in `output` with the same name.
Spark saves files with random names, so I renamed them to initial one.

`config.yaml` file contains information about paths to required columns in XML and target MCC and MNC.

## Execute run_tests.sh file to run tests. Docker is required
No all functions covered with tests, as it's just example

## From my POV, this app should work on events
Each event should contain:
* input file name (location)
* XML schema details (or were they can be found)
* transformation config (or were it can be found)
* output location

# Task 2
Solution with SQL query can be found in `SQL_task.sql` file. 
There were made some assumptions because of lack of knowledge of the database.
Happy to discuss them all.
About optimisation: from my POV, it's possible to speed up query by changing tables structure, as adding indexes etc 
(if they don't exist)
