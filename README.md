# Data Driven Legislation Tracker

### What are we building? 

This is an effort that spawned from a partnership with the American Civil Liberties Union (ACLU). 
In this effort, we are developing an initial version of a data-driven legislative tracking system for state-level legislatures. 
The system aims to produce additional information about bills that are active in legislative sessions, using the trends learned from data about legislative activity and legislative bodies in from the past. 
In the current form, the legislation tracker performs two analyses and produces two types of information about a given bill:
- The likelihood of being passed into law within the current session [Learn more about the bill passage model](docs/bill_passage.md)
- The topic areas to which the bill belongs [Learn more about the issue area classification model](docs/issue_classification.md)

### Why are we developing a legislative tracker?

For advocacy organizations like the ACLU, tracking state-level legislation is a necessary step to identify where to focus and prioritize their advocacy resources.
Predicting passage of a legislative bill can be difficult for humans, even for experts at advocacy organizations, as success of a bill depends on a range of different factors. 
Assigning legislative bills to their respective topic areas, while easier than predicting passage for a domain expert, often requires significant manual efforts to keep up with legislation. 
These manual effor demands often results in a trade-off between recall (amount of identified important legislation) and efficiency (time taken to identify important legislation) of the process.
A data-driven legislation tracking system that predicts passage likelihood, and classifies bills into their topic areas can help advocates identify important legislation improving both recall and efficiency of the process.

### What data are we using?

We used publicly available legislative data for ous analysis.
To obtain well-organized legislative data we partnered up with LegiScan.
[LegiScan](https://legiscan.com/) is an online service that collects legislative session data and diseminates the data through their [API](https://legiscan.com/legiscan). 

We use the Legiscan API to collect data from the state-legislatures from the 50 states. 
We refresh our data every week to capture new legislative activity. 

[Learn more about the data and the storage architecture we used](docs/data_description.md)

### How can you use the tracker?

We are interested in developing a publicly accessible tool that can help any concerned individual to easily track legislative activity in their state. 
The outputs from our legislative tracking system is made available to the public at [URLGOESHERE](www.dssg.io). 

Further, we will make a spreadsheet version of our results available every week at [URLGOESHERE](www.dssg.io).

You can read more about how to use the tool [here](www.dssg.io)

### How can you contribute?

There are two main ways you can contribute to the project:
1. Labeling data
2. Flagging misclassifications

For our topic classification, we are relying on a handful of manually labeled bills to train our classifiers. That means our models can be wrong some (or most) of the time. Flagging bills that are misclassified and indicating the correct label would help us improve our machine learning models. 
To the same end, helping us with more labeled data with their topic areas can increase our training data size and imrove our models. 


