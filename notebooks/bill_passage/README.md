### Notebooks – Passage Prediction

We organize the notebooks into four sections. 

**Analysis**

This folder contains notebooks analysing the label and cohort queries used for passage prediction. Contains info about label distributions across different cohorts, cohort sizes etc. 

**Model Selection**

Contains notebooks that detail how we narrowed down models from the larger model grid. As we used _Triage_ to run our ML pipeline, we used [_Audition_](https://dssg.github.io/triage/audition/audition_intro/),
the model selection module of Triage. 

Important notebook – _audition_notebook.ipynb_

The audition notebook takes an experiment hash (from triage) and compares all model groups against each other for all time splits. More details about using Audition can be found [here](https://github.com/dssg/triage/blob/master/src/triage/component/audition/Audition_Tutorial.ipynb). 


**Postmodeling**

In postmodeling, we take a deeper dive into the trained models. As we want to take an in-depth look, we only consider the models that are narrowed down using _Audition_. 

The postmodeling folder contains several notebooks that analyse factors such as:

- Performance over time of models (precision-recall curves, score distributions, ROC)
- Feature importance for supported model types
- Crosstabs, where we analyse the differences in means for input features between each passage category
- How the feature values of important features correlate with the passage score









