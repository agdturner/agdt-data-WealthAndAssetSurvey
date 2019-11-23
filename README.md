# Wealth and Assets Survey Data Processing Library

https://github.com/agdturner/agdt-java-generic-data-WealthAndAssetsSurvey

A Java library for processing UK Office for National Statistics Wealth and Assets Survey (WaAS) data.

## Usage
This library is used in the following projects:
1. https://github.com/agdturner/agdt-java-project-UKFinancialInequality
A project exploring contemporary UK financial inequalities - specifically looking at relative and absolute changes in wealth for different groups since around 2006.
2. https://github.com/agdturner/agdt-java-project-UKHousingInequality
A project exploring contemporary UK housing inequality - specifically looking at relative and absolute changes in housing wealth and costs for different groups since around 2006.

If you use this library please add to this list.

## Code development
Some of the code that forms part of this library was generated using https://github.com/agdturner/agdt-java-generic-data-WealthAndAssetsSurveyCodeGenerator. Specifically the code found in the following packages:
1. https://github.com/agdturner/agdt-java-generic-data-WealthAndAssetsSurvey/tree/master/src/main/java/uk/ac/leeds/ccg/andyt/generic/data/waas/data/hhold
2. https://github.com/agdturner/agdt-java-generic-data-WealthAndAssetsSurvey/tree/master/src/main/java/uk/ac/leeds/ccg/andyt/generic/data/waas/data/person

The WaAS is described on the ONS Website:
https://www.ons.gov.uk/peoplepopulationandcommunity/personalandhouseholdfinances/debt/methodologies/wealthandassetssurveyqmi

Waves 1 to 5 are available for academic research via the UKDS:
https://beta.ukdataservice.ac.uk/datacatalogue/studies/study?id=7215

Wave 6 is due for release in 2019.

Each wave of the data has two files:
1. A household file which contains variables about each household in the survey.
2. A person file which contains variables bout some persons within each household in the survey.

Each type of file has quite a lot of variables and there are some subtle changes in variables between waves.

## Dependencies
Please see the pom.xml for details.

## Contributions
Please raise issues and submit pull requests in the usual way. Contributions will be acknowledged.

## LICENCE
Please see the standard Apache 2.0 open source LICENCE.
