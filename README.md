# Algorithms for Massive Datasets Project 

This repository contains the python3 code and report required for the final
project of the _Algorithms for Massive Datasets_ course @ UniMi - A.A 2025/2026.

## Deliverables

### Project Choice

The project presented in this repository refers to the following description.

```
The task is to implement from scratch at least two of the algorithms for
stream analysis described during the course, interpreting either the set
of all comments or of all articles in the dataset as a stream. You are free
to decide the problem to be solved, e.g., counting the number of distinct
users, or computing the second moment for the section of the article being
commented, or building a Bloom Filter for the articles of a given section.
```

Following this project description, the submission contains the following implementations:

- Flajoret-Martin algorithm to estimate the number of unique users in a stream.
- AMS algorithm to estimate the second moment of the sections that received a
  comment in the dataset.
- Bloom filter implementation for the users that left a comment, tested against
  the stream of users that left a comment under articles of the _Opinion_ section.

### Source Code

The source code is provided as a Python3 notebook and is meant to be executed primarily inside _Google CoLab_,
running the code locally may require the user to install some libraries or modify
certain parts of the code

<a href="https://colab.research.google.com/github/AndreaTgc/unimi-project-algorithms-for-massive-datasets/blob/main/project.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

### Documentation

The documentation is provided as a *.pdf* file generated from Typst souce, said source(s) can be found in the relative subdirectory (docsrc) for inspection.
Typst is a modern typesetting framework that provides the same functionalities as LaTeX (and more).

## Plagiarims and AI use statement

_I declare that this material, which I now submit for assessment, is entirely my own work and has not been taken from the work of others, save and to the extent that such work has been cited and acknowledged within the text of my work. I understand that plagiarism, collusion, and copying are grave and serious offences in the university and accept the penalties that would be imposed should I engage in plagiarism, collusion or copying. This assignment, or any part of it, has not been previously submitted by me or any other person for assessment on this or any other course of study. No generative AI tool has been used to write the code or the report content._
