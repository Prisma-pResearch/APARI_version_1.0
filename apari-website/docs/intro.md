---
sidebar_position: 1
descption: This document is trying to record the progress of APARI project and provide an reference for other sites. 
---

# Introduction

## Background

APARI (Aligning Patient Acuity with Resource Intensity) project was initiated by Dr. Tyler Loftus in 2022 with the aim of establishing a paradigm for data-driven methods to augmentment postoperative resource intensity decisions. The first phase of the project was developing and validating deep-learning model trained within the Univeristy of Florida Health (UF Health) system (Gainesville and Jacksonville Hospitals), which was shown to exibit good performance in predicting in-hospital mortality and prologned icu stay, 0.92 (95% CI, 0.91-0.93) and 0.92 (95% CI, 0.92-0.92), respectively.
<br />
<br />
The next phase of the project aims to improve performance of the model and extend its application to additional health systems (Fairview Health, IU Health, Emory Healthcare) by leveraging Federated Learning. With Federated Learning, healthcare institutions can collaborate and train a machine learning model using their individual patient data without compromising patient privacy or data security. The Federated APARI model can then be used at eadh institution to predict patient acuity and resource intensity, allowing healthcare providers to make better-informed post-operative triage decisions.

### Project Roadmap

(2023-07-24): Project Launch Meeting
<br />
(2023-09-01): Sites share results of APARI Model trained and Validated locally at their respective institutions

## Documents sections

This online documentation is consisted with below 6 sections to explain how to set up a Federated Learning application between multiple sites

- Data Preparation
- Local Model Training/Validation
- Local Federated Learing simulation
  - Introduction of NVIDIA Flare platform
  - Configure the local FL siulation environment
  - Start local simulation of Federated Learning application with local data 
- Federated learning Provision
  - Introduction of NVIDIA Flare dashboard 
  - Start federated learning provision
- Federated Model Training 
- Model Testing
  - Running cross validation between sites 
