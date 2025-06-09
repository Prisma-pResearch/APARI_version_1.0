
* [Model Toolbox](https://github.com/Prisma-pResearch/Model_Toolbox)


<p align="center">
  <a href="https://example.com/">
    <img src="https://via.placeholder.com/72" alt="Logo" width=72 height=72>
  </a>

  <h3 align="center">Logo</h3>

  <p align="center">
    Module Name Here
    <br>
    <a href="https://github.com/Prisma-pResearch/Model_Toolbox/issues/new?template=bug.md">Report bug</a>
    ·
    <a href="https://github.com/Prisma-pResearch/Model_Toolbox/issues/new?template=feature.md&labels=feature">Request feature</a>
  </p>
</p>


## Table of contents

- [Quick start](#quick-start)
- [Status](#status)
- [Change Log](#change-log)
<!-- - [What's included](#whats-included) -->
- [Overview](#overview)
- [Bugs and feature requests](#bugs-and-feature-requests)
- [Contributing](#contributing)
- [Requirements](#module-requirements)
- [Creators](#creators)
- [Thanks](#thanks)
- [Copyright and license](#copyright-and-license)


## Quick start

This is a general module designed to faciliate the building of other modules.

This module specializes in generalizable and standardized IO from/to Files and/or SQL databases, logging, and dataformatting.

## Status

Production (Complete documentation for some functions may still be needed)

## Change log

(2023-07-11)
* Bug fixes for depracations of PyTorch-Lightning
* Bug fixes for depreacations of numpy methods in DataSet


<!-- ## What's included -->

<!-- Use Utilities.Documentation.markdown_generator.generate_file_structure to update the below tree -->

 

## Overview
*Python/DataSet.py*
* Dataset (class to manage data from an H5 file and pass it to a pytorch dataloader or export as pandas dataframe for SciKitLearn Models. **very usefull**)

*Python.Pytorch.Model_and_Layers.py*
* Model (Dynamic Model with static and time series inputs that can generate continous or single predictions and automatically adjust configuration based on Dataset input. **very useful**)
* Interpretable_Model (Adjunct to the Model Class that enables it to be run through traditional interpretablity metrics (e.g. Integrated Gradients and shap) **very useful**)

*Python.Pytorch.Training.py*
* train_model (Train and evaluate a model based on a configuation dictionary. See bottom of file for example of how it can be used. **very useful**)

*Python.Pytorch.model_interpretability.py*
* Collection of function to facilitate Feature importance calculation and other attributions for models. An example can be seen at the bottom of the file.

*Python.model_metrics.py*
* compute_model_performance (Compute model performance metrics ('AUROC', 'AUPRC', 'Sensitivity', 'Specificity', 'PPV', 'NPV', 'Accuracy', 'F1', 'Youdon-Index') with confidence intervals using bootstrapping for binary outcomes. **very usefull**)
* plot_AUC (plot AUROC curve)
* plot_AUPRC (plot AURPRC curve)

*Python/Scikit_learn/RF/RF_model.py*
* fitRandomForestClassifiers (fit randomForest Classifier(s) on a dataset.)

## Bugs and feature requests

Have a bug or a feature request? Please first read the [issue guidelines](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/CONTRIBUTING.md) and search for existing and closed issues. If your problem or idea is not addressed yet, [please open a new issue](https://github.com/Prisma-pResearch/Model_Toolbox/issues/new).

## Contributing

Please read through our [contributing guidelines](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/CONTRIBUTING.md). Included are directions for opening issues, coding standards, and notes on development.

Moreover, all HTML and CSS should conform to the [Code Guide](https://github.com/mdo/code-guide), maintained by [Main author](https://github.com/ruppert20).

Editor preferences are available in the [editor config](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/.editorconfig) for easy use in common text editors. Read more and download plugins at <https://editorconfig.org/>.

## Module requirements
* Please read [requirements.txt](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/requirements.txt) for required python packages if installing via pip
* Alternatively, the enviroment can be configured via create a conda enviroment according the the [environment.yaml](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/enviroment.yaml) file.

## Creators

**Main Author**
- [Matthew Ruppert](https://github.com/ruppert20) <br />

**Maintainer** <br />
- [Matthew Ruppert](https://github.com/ruppert20)

## Thanks

Some Text

## Copyright and license

Code and documentation copyright 2011-2023 IC3. Code released under the [MIT License](https://github.com/Prisma-pResearch/Model_Toolbox/blob/master/LICENSE).

