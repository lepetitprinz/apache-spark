package transform.aggregate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PointAttributionUdaf extends UserDefinedAggregateFunction {
    private static Logger log = LoggerFactory.getLogger(PointAttributionUdaf.class);

    private static final long serialVersionUID = -66830400L;

    public static final int MAX_POINT_PER_ORDER = 3;

    /**
     * Describes the schema of input sent to the UDAF. Spark UDAFs can operate on any number of columns.
     */
    @Override
    public StructType inputSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(
            DataTypes.createStructField("_c0", DataTypes.IntegerType, true));
        return DataTypes.createStructType(inputFields);
    }

    /**
     * Describes the schema of UDAF buffer.
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(
            DataTypes.createStructField("sum", DataTypes.IntegerType, true));
        return DataTypes.createStructType(bufferFields);
    }

    /**
     * Indicates the datatype coming from the aggregation function
     */
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;  // Datatype of the final result
    }

    /**
     * Describes whether the UDAF is deterministic or not.
     *
     * As Spark executes by splitting data, it processes the chunks separately
     * and combining them. If the UDAF logic is such that the result is
     * independent of the order in which data is processed and combined then
     * the UDAF is deterministic.
     */
    @Override
    public boolean deterministic() {
        return true;  // This function does not care about the order of execution
    }

    /**
     * Initializes the buffer. This method can be called any number of times
     * of Spark during processing.
     *
     * The contract should be that applying the merge function on two initial
     * buffers should just return the initial buffer itself, i.e.
     * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        log.trace("-> initialize() - buffer as {} row(s)", buffer.length());
        buffer.update(
            0, // Column number, starting at 0
            0);   // Initial value for this column
    }

    /**
     * Updates the buffer with an input row. Validations on input should be
     * performed in this method.
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        log.trace("-> update(), input row has {} args", input.length());
        if (input.isNullAt(0)) {
            log.trace("Value passed is null.");
            return;
        }
        log.trace("-> update({}, {})", buffer.getInt(0), input.getInt(0));

        int initialValue = buffer.getInt(0);
        int inputValue = input.getInt(0);
        int outputValue = 0;
        if (inputValue < MAX_POINT_PER_ORDER) {
            outputValue = inputValue;
        } else {
            outputValue = MAX_POINT_PER_ORDER;
        }
        outputValue += initialValue;

        log.trace(
            "Value passed to update() is {}, this will grant {} points",
            inputValue,
            outputValue);
        buffer.update(0, outputValue);
    }

    /**
     * Merges two aggregation buffers and stores the updated buffer values
     * back to buffer.
     */
    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        log.trace("-> merge({}, {})", buffer.getInt(0), row.getInt(0));
        buffer.update(0, buffer.getInt(0) + row.getInt(0));
    }

    /**
     * Calculates the final result of this UDAF based on the given aggregation
     * buffer.
     */
    @Override
    public Object evaluate(Row row) {
        log.trace("-> evaluate({})", row.getInt(0));
        return row.getInt(0);
    }

    /**
     * Describes the schema of input sent to the UDAF
     */

}
