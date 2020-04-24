# Data Set Interfaces
DataSets are the core concept for supplying data to charts and also to operate on the data.

## Basic Interface

A dataset in general is a table where each row conresponds to a n-dimensional point.
Data can be obtained for an index, or as a bunch operation for the complete dataset.


```
Interfaces:
DataSet
```

## Structured Data

Often the individual points are not independent of each other but are structured, e.g.
on a grid.
There are some interfaces and applications which exploit this structuring and are also
used by the different renderers to decide if they are able to render a DataSet or to render it more efficiently.

```
Interfaces:
GridDataSet
EquidistantGridDataSet
(TriangularMeshDataSet?)
```

## Modifications to DataSets

Some DataSets allow to modify the Data.
As the modifications depend on the Structure of the DataSet, the class hirarchy is different, e.g. an `EditableGridDataSet` is not an instance of `EditableDataSet`.

Also the editability Interfaces provide a way to specify constraints on the data.

```
Interfaces:
EditableDataSet
EditableGridDataSet
```

## Locking and Thread safety

Interface for DataSets, which allow locking, providing a way to determine if a DataSet is lockable and to obtain the lock.

```
Interfaces:
LockableDataSet
```

## Errors

Specifies how DataSets provide Errors.

## Axes Descriptions (Range, Label, Unit, Dimension?)

## Histograms?

## Meta Data

# DataSet Implementations
- basic
    - DoubleErrorDataSet
    - DoubleDataSet
    - MultiDimDoubleDataSet
    - MultiDimDoubleErrorDataSet?
    - +Float Types
- special data handling
    - Circular
    - Histogram(2)
    - Fragmented
    - Rolling
- math
    - Averaging
    - Transposed
    - Wrapped
    - Math DataSet (in math library)
