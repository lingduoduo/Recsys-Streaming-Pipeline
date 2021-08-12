### Abstract Algebra for Scala.

[View on GitHub](https://github.com/twitter/algebird)

- [Type Classes](https://twitter.github.io/algebird/typeclasses.html)
- [Data Types](https://twitter.github.io/algebird/datatypes.html)
- [Resources for Learners](https://twitter.github.io/algebird/resources_for_learners.html)
- [FAQ](https://twitter.github.io/algebird/faq.html)
- [Contributing](https://twitter.github.io/algebird/contributing.html)
- [Cookbook](https://twitter.github.io/algebird/cookbook.html)

Algebird is a library which provides abstractions for abstract algebra in the [Scala programming language](https://scala-lang.org/).

This code is targeted at building aggregation systems (via [Scalding](https://github.com/twitter/scalding), [Apache Storm](http://storm.apache.org/) or [Summingbird](https://github.com/twitter/summingbird)). It was originally developed as part of Scalding’s Matrix API, where Matrices had values which are elements of Monoids, Groups, or Rings. Subsequently, it was clear that the code had broader application within Scalding and on other projects within Twitter.

### What can you do with this code?

```
import com.twitter.algebird._
// import com.twitter.algebird._

import com.twitter.algebird.Operators._
// import com.twitter.algebird.Operators._

Map(1 -> Max(2)) + Map(1 -> Max(3)) + Map(2 -> Max(4))
// res0: scala.collection.immutable.Map[Int,com.twitter.algebird.Max[Int]] = Map(2 -> Max(4), 1 -> Max(3))
```

In the above, the class `Max[T]` signifies that the `+` operator should actually be `max` (this is accomplished by providing an implicit instance of a typeclass for `Max` that handles `+`).

- Model a wide class of “reductions” as a sum on some iterator of a particular value type. For example, average, moving average, max/min, set union, approximate set size (in much less memory with HyperLogLog), approximate item counting (using CountMinSketch).
- All of these combine naturally in tuples, vectors, maps, options and more standard scala classes.
- Implementations of Monoids for interesting approximation algorithms, such as Bloom filter, HyperLogLog and CountMinSketch. These allow you to think of these sophisticated operations like you might numbers, and add them up in hadoop or online to produce powerful statistics and analytics.

## Using Algebird

Algebird modules are available on Maven Central. The current groupid and version for all modules is, respectively, `"com.twitter"` and `0.12.3`.

See [Algebird’s page on the Scaladex](https://index.scala-lang.org/twitter/algebird) for information on all published artifacts and their associated Scala versions. Algebird currently supports Scala 2.10 and 2.11.

## Documentation

The latest API docs are hosted at Algebird’s [ScalaDoc index](https://twitter.github.io/algebird/api/).

## Get Involved + Code of Conduct

Pull requests and bug reports are always welcome! Check out our [Contributing guide](https://twitter.github.io/algebird/contributing.html) for information on what we most need help with and how you can get started contributing.

Discussion occurs primarily on the Gitter channel: [![Chat](https://badges.gitter.im/twitter/algebird.svg)](https://gitter.im/twitter/algebird?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/algebird/issues).

We use a lightweight form of project governance inspired by the one used by Apache projects.

Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.

The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](https://github.com/twitter/algebird/blob/develop/COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](https://github.com/twitter/algebird/blob/develop/COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](
