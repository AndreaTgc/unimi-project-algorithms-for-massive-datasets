#import "style.typ": *

#show link: underline

#show: paper.with(
  title : "Andrea Colombo - Algorithms for Massive Datasets course project - A.A 2025/2026",
  authors: ("Andrea Colombo",),
  abstract: [
    This report contains the documentation related to the project submission for the _Algorithms for Massive
    Datasets_ course.
  Taught by #link("https://malchiodi.di.unimi.it/it")[_Dario Malchiodi_] at _Università degli studi di Milano_.\
  We present the implementation of two stream analysis algorithms: Flajolet-Martin and Alon-Matias-Szegedy, starting from the theory behind them and going all the way through the implementation choices and experimental results.
],
)

= Introduction <intro>
\
The analysis of massive datasets has become an increasingly important problem
in the later years. These scenarios pose significant challenges due to the sheer amount of data to be proceseed; in most cases, storing the entire dataset
in memory is not feasible. \
Stream analysis algorithms address this issue by processing the dataset sequentially, using
limited amounts of memory and providing estimations about the stream properties we are
interested in.
Since these algorithms do not need to store the entirety of the stream in memory, they are used
across a moltitude of domains and hardware, ranging from huge server rigs to tiny embedded
microcontrollers.

The algorithms and tasks we are going to explore are the following:

- Using the _Flajolet-Martin Algorithm_ @flajolet1985probabilistic to produce
  and estimation of the unique userIDs present in the dataset.
  The full description of the algorithm and the implementation choices can be
  found in @fm_algo.

== Development Environment
\
This project was developed using _Google Colab_ as the main computation environment. The Jupyter Notebook submitted alongside this project may require some modifications before being run in a local environment. \
Below is a brief description of the computational resources available on the 
CoLab runtime and the versions of the main libraries used in this project:


- *CPU*: Intel Xeon CPU with 2 vCPUs (virtual CPUs) and 13GB of RAM. 
- *Python Version*: 3.13.15 (built with GCC 11.4.0)
- *PySpark Version*: 4.0.4 
- *xxHash Version*: 4.0.1

#boxed-note(
  "Please note that these versions are correct at the time of writing: " + datetime.today().display())

= Dataset Description <dataset_desc>
\
The dataset used for this project is the *New York Times Articles & Comments (2020)* @nyt_articles_comments dataset, freely
available and licensed under the #link("https://it.wikipedia.org/wiki/Licenze_Creative_Commons")[*CC-BY-NC-SA-4.0*] license. \

The datset, once downloaded, has a size of approximately *6.15 GBs* presents itself in the form of multiple files:

- *nyt-articles-2020.csv*: Contains all the articles published in 2020 by the NYT.
- *nyt-comments-2020.csv*: this file contains all the comments relative to the articles found in _nyt-articles-2020.csv_.
- *nyt-comments-part0.csv .. nyt-comments-part9.csv*: these fails simply contain the data found in _nyt-comments-2020.csv_ split
  in 10 different partitions.
- *test.csv*: Not relevant for our use case
- *train.csv*: Not relevant for our use case

== Preprocessing Techniques
\
During the preprocessing phase of the project we discarded all the columns that are not relevant for our use case, in particular:

- For the Flajolet-Martin portion of the project (@fm_algo), we discarded all the columns except the _UserID_ and, since all the
  fields inside the schema are tagged as *nullable*, we excluded all the null entries to avoid using garbage data.

== Subsampling
\
In order to ensure a reasonable execution time, we introduced a method that allows the user to load only a part of the dataset
instead of the whole.
This method can be tweaked by modifying the *SAMPLING_PROPORTION* (see: @sysconf) variable inside the notebook provided alongside this document. \

= System Configuration <sysconf>
\
The python notebook submitted with this project can be configured with the following set of variables:

- *ENABLE_LOGGING*: enables additional prints during the notebook execution
- *SAMPLING_PROPORTION*: How much of the whole dataset we want to use for the current run, valid if in range $(0, 1]$.
- *FM_NUM_HASHES*: Number of hash functions to use for the Flajolet-Martin implementation.

= Flajolet–Martin Algorithm <fm_algo>
\
First introduced in 1985 @flajolet1985probabilistic, the Flajolet-Martin algorithm is a probabilistic
algorithm that aims at estimating the number of distinct elements through the use of hash functions. \
The core idea is to leverage two very important properties of hash functions:

- *Determinism*: the hash function always maps the same value to the same result
- *Uniform Distribution*: the hashed values are uniformly distributed over the binary space

But why do we focus on the number of trailing zeros (tail length) of the hashed value? \
The probability that a single hash value ends with _n_ trailing zeros is $1/(2^n)$. \
From this forumula we can derive that:

- The probability of a single hash value not having _n_ trailing zeros is $1 - 1/2^n = 1 - 2^(-n)$
- The probability of *none* of _k_ hash values having _n_ trailing zeros is $(1 - 1/2^n)^k$. This can be
  approximated to $e^(-k 2^(-n))$ for very large numbers.

Therefore, if the maximum number of trailing zeros is $R_max$, it implies that we have seen approximatively
$2^(R_max)$ unique elements so far inside the stream.

== The Outlier Problem
\
Using a single hash function has one big issue, since we are taking the maximum number of trailing zeros
from a single source, we are susceptible to outliers.

The solution is simply using multiple hash functions and compute the median of their results. This has been verified
to provide more accurate results but it still has one problem, it always results in estimates that are power of 2. \
In order to fix this issue, we compute the average of the median estimations we just computed.

== Space/Time Complexity
\
This algorithm has a time complexity of $O(k n)$ where _n_ is the length of the stream and _k_ is the constant value
that refers to the computational cost of the hash functions and update procedure. \
The space complexity is $O(k)$ instead, we only need to store a constant amount of information needed to update the
trailing zeros counts; the precise memory usage depends on how many hash function we decide to use for the algorithm. \

== Implementation Details

=== Hash Function Choice
\
The hash function used in the submitted implementation is #link("https://xxhash.com/")[xxhash],
an extremely fast non cryptographic hashing algorithm. In particular, this implementation uses
the xxh3 64 bits version, xxhash is one of the most used hashing algorithms for non
cryptographic uses in real world use cases (PySpark uses it too).

=== Algorithm implementation

The portion of the algorithm responsible for computing the tail lengths and
keeping track of the maximum values can be described with 3 simple functions.
Each partition yields a sequence of length _FM_NUM_HASHES_ that is then used
to compute the final estimation.

#code-block(
  lang: "python",
  ```python
  def ctz(x):
      if x == 0:
          return 64
      return (x & -x).bit_length() - 1

  def hash64bits(value, seed):
      return xxhash.xxh64(value.to_bytes(8, "little"), seed=seed).intdigest()

  def fm_partition(it):
      registers = [0] * FM_NUM_HASHES
      for user in it:
          for i in range(FM_NUM_HASHES):
              bits = hash64bits(user, i)
              r = ctz(bits)
              if r > registers[i]:
                  registers[i] = r

      yield registers
  ```
)

== Experimental Results
\
= AMS Algorithm
\
== Space/Time Complexity
\
== Implementation Details
\
== Experimental Results
\
= Bloom Filter <bloom_filter_intro>
\
A bloom filter @bloom1970 is a probabilistic hash based data structure that is used for checking whether or
not an element may be in the dataset while also being ok with having some false positives. \
In this project we implemented this data structure to test it against the stream of
unique _userIDs_ of the people who left at least one comment on articles of the
_Opinion_ category (@dataset_desc).

The bloom filter is structured as follows: \

- An array of $n$ bits is used to store information about the elements seen inside a
  stream or a generic dataset.
- $k$ hash functions are used to map an element to $k$ bit positions.
  - If the bit array already has all those bits set to one, then we may have already
    seen the element. We are still susceptible to false positives because, since the
    bit array size is finite, multiple hashes (mapped with the modulo inside the bit
    array) may end up resulting in the same bit positions being used.
  - If *at least one* of those bit positions is not 1, then the element was never
    encountered before (there can be no false negatives).

It is clear that the performance of the filter itself depends heavily on the number
of bits used for the array and the number of hash functions used on every element.
These parameters heavily depend on the use case and the available resources for the
filter.

== False Positives Theory
\
To understand why the bloom filter is used, we must also go into the math behind
the number of false positives. \
For the rest of this section, we are going to assume _m_ as the number of bits
and _k_ as the number of hash functions used in the filter. \
We are also going to assume that the selected hash functions map each element
uniformely over the _m_ bits.

- After a single element insertion, the probability that a specific bit is
  not going to be set to 1 as a result of the _k_ hash functions is:
    - $P("bit remains 0") = (1 - 1/m)^k$

- Given the probabilities described in the first point, for _n_ insertions,
  the probability that a bit is not going to be set to 1 is:
    - $P("bit remains 0 after n") = (1 - 1/m)^(k n)$

- The probability of a false positive then becomes:
    - $P("false positive") = (1 - (1 - 1/m)^(k n))^k$

- We can now approximate $(1 - 1/m)^(k n)$ to $e^((-k n)/m)$ to obtain:
    - $P("false positive approx") = (1 - e^((-k n)/m))^k$

We can see that, the higher _m_ and _k_ are, the less likely the filter is to
report a false positive when it's being used. \
It goes without saying that we cannot simply keep driving these numbers up
without encountering memory and time issues. To correctly make use of a bloom
filter, the user must choose _m_ based on their memory constraints and _k_
depending on the time that is allocatable to the task of computing the hash
functions for a given element.

== Space/Time Complexity
\
When it comes to time complexity, both checking for an element and inserting
a new one is equal to $O(k)$, where _k_ is the number of hash functions used
in the filter implementation. The time performance of a bloom filter varies
greatly depending on the complexity of the hash functions used (which is generally
tied to the complexity of the type of the elements to hash).
Generalising this to a stream it becomes trivial that, for _n_ elements, the
complexity goes up to $O(n k)$.
The space complexity of the bloom filter is trivial too, it is equal to $O(m)$
  where _m_ is the number of bits used.

== Implementation Details <bloom_filter_impl>
\
Implementing a bloom filter from scratch in python is very straight forward.
Since python's integers are not restricted to a maximum amount of bits, we can use a
single int as our bit array, this means that accessing individual bits can be done with
simple bitwise operations that are very efficient.
The class is required to have at least the following methods:

In order to use stable hash functions, we can store them in the object state as an array
of _k_ elements, the hash function chosen for this implementation is taken from the xxhash library.

Below is a code snippet to showcase the membership check and insertion methods for the
described class:

#code-block(
  lang: "python",
  ```python
  def add(self, x):
      self.bits |= self._map_bits(x)

  def _map_bits(self, x):
      mask = 0
      for hf in self.hash_fns:
          mask |= (1 << (hf(x) % self.nbits))
      return mask

  def contains(self, x):
      xmask = self._map_bits(x)
      return (self.bits & xmask) == xmask
  ```
)

- self.nbits: number of bits we want to store for the filter (_m_).
- self.hash_fns: array of _k_ hash functions that return a 64 bit integer.
- self.bits: bit array that keeps track of the filter state (implemented using python's
  unbounded ints).

== Chosen Task

As introduced in @bloom_filter_intro, the proposed bloom filter implementation will be
tested against a stream of _UserIDs_ (numerical values) to check for



== Experimental results
\
The bloom filter seen in @bloom_filter_impl has been tested with multiple parameter
configurations (for the number of bits and hash functions) to see how much they influenced
the end performance on the target dataset. \

= Plagiarism and AI Usage Statement
\
_I declare that this material, which I now submit for assessment, is entirely my own work and has not been taken from the work of others, save and to the extent that such work has been cited and acknowledged within the text of my work. I understand that plagiarism, collusion, and copying are grave and serious offences in the university and accept the penalties that would be imposed should I engage in plagiarism, collusion or copying. This assignment, or any part of it, has not been previously submitted by me or any other person for assessment on this or any other course of study. No generative AI tool has been used to write the code or the report content._

#bibliography("bibliography.bib")
