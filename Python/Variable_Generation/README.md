
* [variable Generation](https://github.com/Prisma-pResearch/Variable_Generation)


<p align="center">
  <a href="https://example.com/">
    <img src="https://via.placeholder.com/72" alt="Logo" width=72 height=72>
  </a>

  <h3 align="center">Logo</h3>

  <p align="center">
    Module Name Here
    <br>
    <a href="https://github.com/Prisma-pResearch/Variable_Generation/issues/new?template=bug.md">Report bug</a>
    ·
    <a href="https://github.com/Prisma-pResearch/Variable_Generation/issues/new?template=feature.md&labels=feature">Request feature</a>
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

This is a general module designed to faciliate the Generation of variables at the time of admission, Surgery, or ICU admission..

This Module is designed to run on OMOP data sources only.

## Status

Production (Complete documentation for some functions may still be needed)

## Change log

(2023-07-11)
* Is now fully OMOP compatible and does not rely on value_as_string or procedure_code_soure_value for variable generation tasks
* Requires the use of [setup_OMOP_project_and_download_data](https://github.com/Prisma-pResearch/Utilities/blob/400ecdb74b184cf66db329b94bf65dc2eea5ffe6/ProjectManagement/setup_project.py) in order to run this module
* Includes imporoved Utilities and AKI_Phenotyping Submodules


<!-- ## What's included -->

<!-- Use Utilities.Documentation.markdown_generator.generate_file_structure to update the below tree -->

 

## Overview
*Python/variable_generation_v2.py*
* omop_variable_generation (generate variables using variable specification document)

## Bugs and feature requests

Have a bug or a feature request? Please first read the [issue guidelines](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/CONTRIBUTING.md) and search for existing and closed issues. If your problem or idea is not addressed yet, [please open a new issue](https://github.com/Prisma-pResearch/Variable_Generation/issues/new).

## Contributing

Please read through our [contributing guidelines](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/CONTRIBUTING.md). Included are directions for opening issues, coding standards, and notes on development.

Moreover, all HTML and CSS should conform to the [Code Guide](https://github.com/mdo/code-guide), maintained by [Main author](https://github.com/ruppert20).

Editor preferences are available in the [editor config](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/.editorconfig) for easy use in common text editors. Read more and download plugins at <https://editorconfig.org/>.

## Module requirements
* Please read [requirements.txt](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/requirements.txt) for required python packages if installing via pip
* Alternatively, the enviroment can be configured via create a conda enviroment according the the [environment.yaml](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/enviroment.yaml) file.

## Creators

**Main Author**
- [Matthew Ruppert](https://github.com/ruppert20) <br />

**Maintainer** <br />
- [Matthew Ruppert](https://github.com/ruppert20)

## Thanks

Some Text

## Copyright and license

Code and documentation copyright 2011-2023 IC3. Code released under the [MIT License](https://github.com/Prisma-pResearch/Variable_Generation/blob/master/LICENSE).
