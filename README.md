# Task 1

## In order to run app, you need to execute entrypoint.sh script. Docker is required
Example of proceeded data is in the `results` folder

Application waits for xml files in `input` folder. It makes 5sec sleep after each check. 
Result file will be placed in `output` with the same name.
Spark saves files with random names, so I renamed them to initial one.

I added `return` cmd, so app finish after the 1st file.

`config.yaml` file contains information about paths to required columns in XML and target MCC and MNC.


## From my POV, this app should work on events
Each event should contain:
* input file name (location)
* XML schema details (or were they can be found)
* transformation config (or were it can be found)
* output location
