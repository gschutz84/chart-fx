package de.gsi.dataset.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.gsi.dataset.AxisDescription;
import de.gsi.dataset.DataSet;
import de.gsi.dataset.utils.AssertUtils;

public class DataSetBuilder {
    private static final int MAX_AXIS_DIMENSION = 1000;
    protected String name;
    protected Map<Integer, double[]> values = new HashMap<>();
    protected Map<Integer, double[]> errorsPos = new HashMap<>();
    protected Map<Integer, double[]> errorsNeg = new HashMap<>();
    protected List<String> infoList = new ArrayList<>();
    protected List<String> warningList = new ArrayList<>();
    protected List<String> errorList = new ArrayList<>();
    protected Map<String, String> metaInfoMap = new HashMap<>();
    protected Map<Integer, String> dataLabels = new HashMap<>();
    protected Map<Integer, String> dataStyles = new HashMap<>();
    protected List<AxisDescription> axisDescriptions = new ArrayList<>();
    protected int initialCapacity = -1;
    private int dimension = -1;
    private boolean errors = false;

    /**
     * default DataSet factory
     */
    public DataSetBuilder() {
        super();
    }

    /**
     * default DataSet factory
     *
     * @param dataSetName data set name
     */
    public DataSetBuilder(final String dataSetName) {
        super();
        this.setName(dataSetName);
    }

    protected void addDataLabelStyleMap(final DataSet dataSet) {
        if (!(dataSet instanceof AbstractDataSet)) {
            return;
        }
        AbstractDataSet<?> ds = (AbstractDataSet<?>) dataSet;
        if (!dataLabels.isEmpty()) {
            dataLabels.forEach(ds::addDataLabel);
        }
        if (!dataStyles.isEmpty()) {
            dataStyles.forEach(ds::addDataStyle);
        }
    }

    protected void addDataRanges(final DataSet dataSet) {
        if (axisDescriptions.isEmpty()) {
            return;
        }

        // TODO: check if enough axes are already defined ie. via
        // DataSet::getDimension(...) to be implemented
        dataSet.getAxisDescriptions().clear();
        for (int dimension = 0; dimension < axisDescriptions.size(); dimension++) {
            dataSet.getAxisDescriptions().add(new DefaultAxisDescription(dataSet, getAxisDescription(dimension)));
        }
    }

    protected void addMetaData(final DataSet dataSet) {
        if (!(dataSet instanceof AbstractDataSet)) {
            return;
        }
        AbstractDataSet<?> ds = (AbstractDataSet<?>) dataSet;
        ds.getInfoList().addAll(infoList);
        ds.getWarningList().addAll(warningList);
        ds.getErrorList().addAll(errorList);
        ds.getMetaInfo().putAll(metaInfoMap);
    }

    public DataSet build() {
        final String dsName = name == null ? ("DataSet@" + System.currentTimeMillis()) : name;
        DataSet dataSet = buildRawDataSet(dsName);

        // add meta data
        addMetaData(dataSet);

        addDataRanges(dataSet);

        addDataLabelStyleMap(dataSet);

        return dataSet;
    }

    protected DataSet buildRawDataSet(final String dsName) {
        int maxDim = values.keySet().stream().collect(Collectors.maxBy(Integer::compare)).orElse(-1);
        maxDim = Math.max(maxDim, errorsNeg.keySet().stream().collect(Collectors.maxBy(Integer::compare)).orElse(-1));
        maxDim = Math.max(maxDim, errorsPos.keySet().stream().collect(Collectors.maxBy(Integer::compare)).orElse(-1));
        int minArrays = values.values().stream().map(e -> e.length).collect(Collectors.maxBy(Integer::compare)).orElse(-1);
        minArrays = Math.max(minArrays, errorsNeg.values().stream().map(e -> e.length).collect(Collectors.maxBy(Integer::compare)).orElse(-1));
        minArrays = Math.max(minArrays, errorsPos.values().stream().map(e -> e.length).collect(Collectors.maxBy(Integer::compare)).orElse(-1));
        final int size = initialCapacity < 0 ? minArrays : Math.min(minArrays, initialCapacity);
        if (this.dimension == -1) {
            this.dimension = maxDim + 1;
        } else if (this.dimension <= maxDim) {
            throw new IllegalStateException("Supplied data dimensions exceed requested number of dimensions");
        }
        switch (this.dimension) {
        case 0:
        case 1:
        case 2:
            if (values.size() == 0 && errorsNeg.size() == 0 && errorsPos.size() == 0 && initialCapacity <= 0) {
                return new DefaultDataSet(dsName, Math.max(initialCapacity, 0));
            }
            return build2dDataSet(dsName, size);
        case 3:
            //            return build3dDataset(dsName, size);
        default:
            return buildMultiDimDataSet(dsName, size, dimension);
        }
    }

    private DataSet build2dDataSet(final String dsName, final int size) {
        if (errorsNeg.size() == 0 && errorsPos.size() == 0 && !this.errors)
            return buildDefaultDataSet(dsName, size);
        return buildDefaultErrorDataSet(dsName, size);
    }

    private DataSet buildDefaultErrorDataSet(final String dsName, final int size) {
        double[] xvalues = values.get(DataSet.DIM_X);
        if (xvalues == null) {
            xvalues = IntStream.range(0, size).mapToDouble(x -> x).toArray();
        }
        double[] yvalues = values.get(DataSet.DIM_Y);
        if (yvalues == null) {
            yvalues = new double[size];
        }
        if (errorsNeg.containsKey(DataSet.DIM_X) || errorsPos.containsKey(DataSet.DIM_X)) {
            throw new IllegalStateException("DataSetBuilder: X Errors not implemented for 2D DataSetBuilder");
        }
        double[] yen = errorsNeg.get(DataSet.DIM_Y);
        if (yen == null) {
            yen = new double[size];
        }
        double[] yep = errorsPos.get(DataSet.DIM_Y);
        if (yep == null) {
            yep = new double[size];
        }
        return new DefaultErrorDataSet(dsName, xvalues, yvalues, yen, yep, size, false);
    }

    private DataSet buildDefaultDataSet(final String dsName, final int size) {
        double[] xvalues = values.get(DataSet.DIM_X);
        if (xvalues == null) {
            xvalues = IntStream.range(0, size).mapToDouble(x -> x).toArray();
        }
        double[] yvalues = values.get(DataSet.DIM_Y);
        if (yvalues == null) {
            yvalues = new double[size];
        }
        return new DefaultDataSet(dsName, xvalues, yvalues, size, false);
    }

    private DataSet build3dDataset(final String dsName, final int size) {
        double[] xvalues = values.get(DataSet.DIM_X);
        int xsize = 0; // TODO: determine real size
        if (xvalues == null) {
            xvalues = IntStream.range(0, xsize).mapToDouble(x -> x).toArray();
        } else {
            xsize = xvalues.length;
        }
        double[] yvalues = values.get(DataSet.DIM_Y);
        int ysize = 0; // TODO: determine real size
        if (yvalues == null) {
            yvalues = IntStream.range(0, ysize).mapToDouble(x -> x).toArray();
        } else {
            ysize = yvalues.length;
        }
        double[] zValuesLinear = values.get(DataSet.DIM_Z);
        if (xsize == ysize && xsize == zValuesLinear.length) {
            return buildMultiDimDataSet(dsName, xsize, 3);
        }
        double[][] zValues = new double[ysize][];
        if (zValuesLinear == null) {
            zValuesLinear = new double[xvalues.length * yvalues.length];
        } else {
            AssertUtils.checkArrayDimension("zData", zValuesLinear, xvalues.length * yvalues.length);
            for (int i = 0; i < ysize; i++) {
                zValues[i] = new double[xsize];
                System.arraycopy(zValuesLinear, i * xsize, zValues[i], 0, xsize);
            }
        }
        return new DoubleDataSet3D(dsName, xvalues, yvalues, zValues);
    }

    private DataSet buildMultiDimDataSet(final String dsName, final int size, final int nDims) {
        final double[][] inputValues = new double[nDims][];
        int initialSize = this.initialCapacity;
        for (int dimIndex = 0; dimIndex < nDims; dimIndex++) {
            double[] val = values.get(dimIndex);
            if (val == null) {
                if (dimIndex == DataSet.DIM_X) {
                    val = IntStream.range(0, initialSize).mapToDouble(x -> x).toArray();
                } else {
                    val = new double[initialSize];
                }
            } else if (val.length < initialSize) {
                final double[] newVal = new double[initialSize];
                System.arraycopy(val, 0, newVal, 0, val.length);
                val = newVal;
            }
            inputValues[dimIndex] = val;
            if (errorsNeg.containsKey(dimIndex) || errorsPos.containsKey(dimIndex)) {
                throw new IllegalStateException("DataSetBuilder: X Errors not implemented for MultiDimDataSet");
            }
        }
        return new MultiDimDoubleDataSet(dsName, inputValues, 0, false);
    }

    private AxisDescription getAxisDescription(int dimension) {
        if (dimension < 0 || dimension >= MAX_AXIS_DIMENSION) {
            throw new IllegalArgumentException(
                    "axis dimension out of range [0, " + MAX_AXIS_DIMENSION + "]: " + dimension);
        }
        if (dimension < axisDescriptions.size()) {
            return axisDescriptions.get(dimension);
        }
        // axis info does not yet exist
        // add as many dummy fields as necessary
        while (dimension >= axisDescriptions.size()) {
            axisDescriptions.add(new DefaultAxisDescription());
        }

        return axisDescriptions.get(dimension);
    }

    public DataSetBuilder setAxisMax(final int dimension, final double value) {
        getAxisDescription(dimension).setMax(value);
        return this;
    }

    public DataSetBuilder setAxisMin(final int dimension, final double value) {
        getAxisDescription(dimension).setMin(value);
        return this;
    }

    public DataSetBuilder setAxisName(final int dimension, final String name) {
        getAxisDescription(dimension).set(name);
        return this;
    }

    public DataSetBuilder setAxisUnit(final int dimension, final String unit) {
        getAxisDescription(dimension).set(getAxisDescription(dimension).getName(), unit);
        return this;
    }

    public DataSetBuilder setDataLabelMap(final Map<Integer, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }
        dataLabels.putAll(map);
        return this;
    }

    public DataSetBuilder setDataStyleMap(final Map<Integer, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }
        dataStyles.putAll(map);
        return this;
    }

    public DataSetBuilder setMetaErrorList(final String[] errors) {
        this.errorList.addAll(Arrays.asList(errors));
        return this;
    }

    public DataSetBuilder setMetaInfoList(final String[] infos) {
        this.infoList.addAll(Arrays.asList(infos));
        return this;
    }

    public DataSetBuilder setMetaInfoMap(final Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }
        metaInfoMap.putAll(map);
        return this;
    }

    public DataSetBuilder setMetaWarningList(final String[] warning) {
        this.warningList.addAll(Arrays.asList(warning));
        return this;
    }

    public final DataSetBuilder setName(final String name) {
        this.name = name;
        return this;
    }

    public DataSetBuilder setNegError(final int dimIndex, final double[] errors) {
        final int size = initialCapacity < 0 ? errors.length : Math.min(initialCapacity, errors.length);
        final double[] vals = new double[size];
        this.errorsNeg.put(dimIndex, vals);
        System.arraycopy(errors, 0, vals, 0, size);
        return this;
    }

    public DataSetBuilder setNegErrorNoCopy(final int dimIndex, final double[] errors) { // NOPMD
        // direct storage is on purpose
        this.errorsNeg.put(dimIndex, errors);
        return this;
    }

    public final DataSetBuilder setPosError(final int dimIndex, final double[] errors) {
        final int size = initialCapacity < 0 ? errors.length : Math.min(initialCapacity, errors.length);
        final double[] vals = new double[size];
        this.errorsPos.put(dimIndex, vals);
        System.arraycopy(errors, 0, vals, 0, size);
        return this;
    }

    public final DataSetBuilder setPosErrorNoCopy(final int dimIndex, final double[] errors) { // NOPMD
        // direct storage is on purpose
        this.errorsPos.put(dimIndex, errors);
        return this;
    }

    public final DataSetBuilder setValues(final int dimIndex, final double[] values) {
        final int size = initialCapacity < 0 ? values.length : Math.min(initialCapacity, values.length);
        final double[] vals = new double[size];
        this.values.put(dimIndex, vals);
        System.arraycopy(values, 0, vals, 0, size);
        return this;
    }

    public final DataSetBuilder setValues(final int dimIndex, final double[][] values) {
        AssertUtils.nonEmptyArray("values", values);
        AssertUtils.nonEmptyArray("values first col", values[0]);
        int ysize = values.length;
        int xsize = values[0].length;
        final int size = initialCapacity < 0 ? ysize * xsize : Math.min(initialCapacity, values.length);
        final double[] vals = new double[size];
        for (int i = 0; i < ysize; i++) {
            AssertUtils.checkArrayDimension("column length", values[i], xsize);
            System.arraycopy(values[i], 0, vals, i * xsize, xsize);
        }
        this.values.put(dimIndex, vals);
        return this;
    }

    public DataSetBuilder setValuesNoCopy(int dimIndex, double[] values) {
        this.values.put(dimIndex, values);
        return this;
    }

    public DataSetBuilder setDimension(final int dimension) {
        this.dimension = dimension;
        return this;
    }

    public DataSetBuilder setEnableErrors(boolean enableErrors) {
        this.errors = enableErrors;
        return this;
    }

    public DataSetBuilder setInitalCapacity(int newInitialCapacity) {
        initialCapacity = newInitialCapacity;
        return this;
    }
}
