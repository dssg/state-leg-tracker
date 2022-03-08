# Labeling Bills for Issue Area Identification

The goal of this process is to tag historical bills that were introduced to state legislatures with their relevant issue areas. 
These labels will serve as the training data for the machine learning models.
We are interested in six issue areas in this project:
- Criminal Justice
- Voting Rights
- Racial Justice
- Immigrants' Rights
- Tech, privacy, and civil liberties
- LGBTQ Rights


## Labeling process

CMU will provide samples of bills to ACLU for labeling. 
The first phase of labeling is done using XLS files. 
The provided XLS file contains the following columns:
- Bill id -- An internal unique identifier for a bill
- doc id -- An internal unique identifier for a bill version, i.e. one bill id can have several doc ids
- Bill number -- The unique idenfier for the bill in the state, and legislative session
- state -- The state the bill belongs to
- URL -- The link where the document can be read
- Columns for issue areas -- A column for each issue area specified above. The relevance of the issue area to the bill is specified in the respective column.

We assume that each bill can belong to multiple issue areas. We propose to use the following set of labels to capture,(1) the issue areas a bill belongs to, and (2) how much of a focus of the bill is on the issue area. 

- _primary_ -- The primary focus area of the bill. A bill can only have one _primary_ issue area
- _major_ -- A major focus area of the bill. A bill can have multiple _major_ issue areas
- _minor_ -- The bill contains only small provisions that relate to the issue area. A bill can have multiple _minor_ issue areas.

**Notes**

- If a bill spans across multiple issue areas and it's difficult to assign a primary focus, the bill can be labeled with multiple _major_ issue areas without a primary focus
- A bill cannot have multiple _primary_ focus areas
- We are labeling each version of the bill independently from each other. Therefore, different versions of the same bill can appear on the list. Please only consider the relevant version's text when assigning labels to a bill

