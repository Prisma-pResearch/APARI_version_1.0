# Contributing guidelines

## Pull Request Checklist

Before sending your pull requests, make sure you do the following:

-   Read the [contributing guidelines](CONTRIBUTING.md).
-   Changes are consistent with the [Coding Style](#c-coding-style).
-   Run the [unit tests](#running-unit-tests).

## How to become a contributor and submit your own code

![Screen Shot 2022-08-30 at 7 27 04 PM](https://user-images.githubusercontent.com/42785357/187579207-9924eb32-da31-47bb-99f9-d8bf1aa238ad.png)

### Typical Pull Request Workflow -

**1. New PR** - As a contributor, you submit a New PR on GitHub. - We inspect
every incoming PR and add certain labels to the PR.

**2. Valid?** - If the PR passes all the quality checks then we go ahead and
assign a reviewer. - If the PR didn't meet the validation criteria, we request
for additional changes to be made to PR to pass quality checks and send it back
or on a rare occassion we may reject it.

**3. Review** - For Valid PR, reviewer (person familiar with the
code/functionality) checks if the PR looks good or needs additional changes. -
If all looks good, reviewer would approve the PR. - If a change is needed, the
contributor is requested to make suggested change. - You make the change and
submit for the review again. - This cycle repeats itself till the PR gets
approved. - Note: As a friendly reminder we may reach out to you if the PR is
awaiting your response for more than 2 weeks.

**4. Approved** - Once the PR is approved, it gets `review:approved` label
applied and it initiates CI/CD tests. - We can't move forward if these tests
fail. - In such situations, we may request you to make further changes to your
PR for the tests to pass.

**5. Copy to G3** - Once the PR is in repository codebase, we make sure it
integrates well with its dependencies and the rest of the system. - Rarely, but
If the tests fail at this stage, we cannot merge the code. - If needed, we may
come to you to make some changes. - At times, it may not be you, it may be us
who may have hit a snag. - Please be patient while we work to fix this. - Once
the internal tests pass, we go ahead and merge the code internally as well as
externally on GitHub.

### Contributor License Agreements

We'd love to accept your contributions and patches!


### Contributing code

If you have improvements to IC3 Repositories, send us your pull requests! For those
just getting started, Github has a
[how to](https://help.github.com/articles/using-pull-requests/).

IC3 team members will be assigned to review your pull requests. Once the
pull requests are approved and pass continuous integration checks, a IC3
team member will apply `ready to pull` label to your change. This means we are
working on getting your pull request submitted to our internal repository. After
the change has been submitted internally, your pull request will be merged
automatically on GitHub.

If you want to contribute, start working through the IC3 Utilities codebase,
navigate to the
[Github "issues" tab](https://github.com/Prisma-pResearch/Utilities/issues) and start
looking through interesting issues. If you are not sure of where to start, then
start by trying one of the  here i.e.
[issues with the "contributions welcome" label](https://github.com/Prisma-pResearch/Utilities/labels/contributors%20welcome).
These are issues that we believe are particularly well suited for outside
contributions, often because we probably won't get to them right now. If you
decide to start on an issue, leave a comment so that other people know that
you're working on it. If you want to help out, but not alone, use the issue
comment thread to coordinate.

### Contribution guidelines and standards

Before sending your pull request for
[review](https://github.com/tensorflow/tensorflow/pulls),
make sure your changes are consistent with the guidelines and follow the
TensorFlow coding style.

#### General guidelines and philosophy for contribution

*   Include unit tests when you contribute new features, as they help to a)
    prove that your code works correctly, and b) guard against future breaking
    changes to lower the maintenance cost.
*   Bug fixes also generally require unit tests, because the presence of bugs
    usually indicates insufficient test coverage.
*   Keep API compatibility in mind when you change code in FileHandling and PreProcessing cores,
    e.g., code in
    [FileHandling](https://github.com/Prisma-pResearch/Utilities/tree/main/FileHandling)
    and
    [PreProcessing](https://github.com/Prisma-pResearch/Utilities/tree/main/PreProcessing).
*   When you contribute a new feature to Utilities, the maintenance burden is
    (by default) transferred to the IC3 team. This means that the benefit
    of the contribution must be compared against the cost of maintaining the
    feature.
*   As every PR requires several CPU/GPU hours of CI testing, we discourage
    submitting PRs to fix one typo, one warning,etc. We recommend fixing the
    same issue at the file level at least (e.g.: fix all typos in a file, fix
    all compiler warning in a file, etc.)
*   Tests should follow the
    [testing best practices](https://www.tensorflow.org/community/contribute/tests)
    guide.

#### License

Include a license at the top of new files.

*   [Python license example](https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/ops/nn.py#L1)
*   [SQL license example](https://github.com/tensorflow/tensorflow/blob/master/tensorflow/java/src/main/java/org/tensorflow/Graph.java#L1)


#### SQL coding style

Changes to IC3 SQL code should conform to
[IC3 SQL Style Guide](https://google.github.io/styleguide/cppguide.html).


#### Python coding style

Changes to IC3 Python code should conform to
[IC3 Python Style Guide](https://github.com/google/styleguide/blob/gh-pages/pyguide.md)

Use `pylint` to check your Python changes. To install `pylint` and check a file
with `pylint` against TensorFlow's custom style definition:

```bash
pip install pylint
pylint --rcfile=tensorflow/tools/ci_build/pylintrc myfile.py
```

Note `pylint --rcfile=tensorflow/tools/ci_build/pylintrc` should run from the
top level tensorflow directory.



<!-- #### Running doctest for testable docstring

There are two ways to test the code in the docstring locally:

1.  If you are only changing the docstring of a class/function/method, then you
    can test it by passing that file's path to
    [tf_doctest.py](https://www.tensorflow.org/code/tensorflow/tools/docs/tf_doctest.py).
    For example:

    ```bash
    python tf_doctest.py --file=<file_path>
    ```

    This will run it using your installed version of TensorFlow. To be sure
    you're running the same code that you're testing:

    *   Use an up to date [tf-nightly](https://pypi.org/project/tf-nightly/)
        `pip install -U tf-nightly`
    *   Rebase your pull request onto a recent pull from
        [TensorFlow's](https://github.com/tensorflow/tensorflow) master branch.

2.  If you are changing the code and the docstring of a class/function/method,
    then you will need to
    [build TensorFlow from source](https://www.tensorflow.org/install/source).
    Once you are setup to build from source, you can run the tests:

    ```bash
    bazel run //tensorflow/tools/docs:tf_doctest
    ```

    or

    ```bash
    bazel run //tensorflow/tools/docs:tf_doctest -- --module=ops.array_ops
    ```

    The `--module` is relative to `tensorflow.python`. -->

