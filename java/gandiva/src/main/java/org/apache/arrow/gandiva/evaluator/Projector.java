/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.EvaluatorClosedException;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ArrowTypeHelper;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This class provides a mechanism to evaluate a set of expressions against a RecordBatch.
 * Follow these steps to use this class:
 * 1) Use the static method make() to create an instance of this class that evaluates a
 *    set of expressions
 * 2) Invoke the method evaluate() to evaluate these expressions against a RecordBatch
 * 3) Invoke close() to release resources
 */
public class Projector {
  private static final org.slf4j.Logger logger =
          org.slf4j.LoggerFactory.getLogger(Projector.class);

  private JniWrapper wrapper;
  private final long moduleId;
  private final Schema schema;
  private final int numExprs;
  private boolean closed;

  private Projector(JniWrapper wrapper, long moduleId, Schema schema, int numExprs) {
    this.wrapper = wrapper;
    this.moduleId = moduleId;
    this.schema = schema;
    this.numExprs = numExprs;
    this.closed = false;
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs)
      throws GandivaException {
    return make(schema, exprs, SelectionVectorType.SV_NONE, JniLoader.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param configOptions ConfigOptions parameter
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
                               ConfigurationBuilder.ConfigOptions configOptions) throws GandivaException {
    return make(schema, exprs, SelectionVectorType.SV_NONE, JniLoader.getConfiguration(configOptions));
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param optimize Flag to choose if the generated llvm code is to be optimized
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  @Deprecated
  public static Projector make(Schema schema, List<ExpressionTree> exprs, boolean optimize)
      throws GandivaException {
    return make(schema, exprs, SelectionVectorType.SV_NONE,
            JniLoader.getConfiguration((new ConfigurationBuilder.ConfigOptions()).withOptimize(optimize)));
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      SelectionVectorType selectionVectorType)
          throws GandivaException {
    return make(schema, exprs, selectionVectorType, JniLoader.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   * @param configOptions ConfigOptions parameter
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs, SelectionVectorType selectionVectorType,
                               ConfigurationBuilder.ConfigOptions configOptions) throws GandivaException {
    return make(schema, exprs, selectionVectorType, JniLoader.getConfiguration(configOptions));
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   * @param optimize Flag to choose if the generated llvm code is to be optimized
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  @Deprecated
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
                               SelectionVectorType selectionVectorType, boolean optimize)
      throws GandivaException {
    return make(schema, exprs, selectionVectorType,
            JniLoader.getConfiguration((new ConfigurationBuilder.ConfigOptions()).withOptimize(optimize)));
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   * @param configurationId Custom configuration created through config builder.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      SelectionVectorType selectionVectorType,
      long configurationId) throws GandivaException {
    return make(schema, exprs, selectionVectorType, configurationId, null);
  }
    
  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema         Table schema. The field names in the schema should match the fields used
   *                       to create the TreeNodes
   * @param exprs          List of expressions to be evaluated against data
   * @param configOptions  ConfigOptions parameter
   * @param secondaryCache SecondaryCache cache for gandiva object code.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      ConfigurationBuilder.ConfigOptions configOptions,
      JavaSecondaryCacheInterface secondaryCache) throws GandivaException {
    return make(schema, exprs, SelectionVectorType.SV_NONE, JniLoader.getConfiguration(configOptions), secondaryCache);
  }
    
  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evaluate() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema              Table schema. The field names in the schema should match the fields used
   *                            to create the TreeNodes
   * @param exprs               List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   * @param configurationId     Custom configuration created through config builder.
   * @param secondaryCache      SecondaryCache cache for gandiva object code.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      SelectionVectorType selectionVectorType,
      long configurationId, JavaSecondaryCacheInterface secondaryCache) throws GandivaException {
    // serialize the schema and the list of expressions as a protobuf
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }

    // Invoke the JNI layer to create the LLVM module representing the expressions
    GandivaTypes.Schema schemaBuf = ArrowTypeHelper.arrowSchemaToProtobuf(schema);
    JniWrapper wrapper = JniLoader.getInstance().getWrapper();
    long moduleId = wrapper.buildProjector(secondaryCache, schemaBuf.toByteArray(),
        builder.build().toByteArray(), selectionVectorType.getNumber(), configurationId);
    logger.debug("Created module for the projector with id {}", moduleId);
    return new Projector(wrapper, moduleId, schema, exprs.size());
  }

  /**
   * Invoke this function to evaluate a set of expressions against a recordBatch.
   *
   * @param recordBatch Record batch including the data
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch, List<ValueVector> outColumns)
          throws GandivaException {
    evaluate(recordBatch.getLength(), recordBatch.getBuffers(),
             recordBatch.getBuffersLayout(),
             SelectionVectorType.SV_NONE.getNumber(), recordBatch.getLength(),
             0, 0, outColumns);
  }

  /**
   * Invoke this function to evaluate a set of expressions against a set of arrow buffers.
   * (this is an optimised version that skips taking references).
   *
   * @param numRows number of rows.
   * @param buffers List of input arrow buffers
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(int numRows, List<ArrowBuf> buffers,
                       List<ValueVector> outColumns) throws GandivaException {
    List<ArrowBuffer> buffersLayout = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      long size = arrowBuf.readableBytes();
      buffersLayout.add(new ArrowBuffer(offset, size));
      offset += size;
    }
    evaluate(numRows, buffers, buffersLayout,
             SelectionVectorType.SV_NONE.getNumber(),
             numRows, 0, 0, outColumns);
  }

  /**
   * Invoke this function to evaluate a set of expressions against a {@link ArrowRecordBatch}.
   *
   * @param recordBatch The data to evaluate against.
   * @param selectionVector Selection vector which stores the selected rows.
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch,
                     SelectionVector selectionVector, List<ValueVector> outColumns)
        throws GandivaException {
    evaluate(recordBatch.getLength(), recordBatch.getBuffers(),
        recordBatch.getBuffersLayout(),
        selectionVector.getType().getNumber(),
        selectionVector.getRecordCount(),
        selectionVector.getBuffer().memoryAddress(),
        selectionVector.getBuffer().capacity(),
        outColumns);
  }

  /**
 * Invoke this function to evaluate a set of expressions against a set of arrow buffers
 * on the selected positions.
 * (this is an optimised version that skips taking references).
 *
 * @param numRows number of rows.
 * @param buffers List of input arrow buffers
 * @param selectionVector Selection vector which stores the selected rows.
 * @param outColumns Result of applying the project on the data
 */
  public void evaluate(int numRows, List<ArrowBuf> buffers,
                     SelectionVector selectionVector,
                     List<ValueVector> outColumns) throws GandivaException {
    List<ArrowBuffer> buffersLayout = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      long size = arrowBuf.readableBytes();
      buffersLayout.add(new ArrowBuffer(offset, size));
      offset += size;
    }
    evaluate(numRows, buffers, buffersLayout,
        selectionVector.getType().getNumber(),
        selectionVector.getRecordCount(),
        selectionVector.getBuffer().memoryAddress(),
        selectionVector.getBuffer().capacity(),
        outColumns);
  }

  private void evaluate(int numRows, List<ArrowBuf> buffers, List<ArrowBuffer> buffersLayout,
                       int selectionVectorType, int selectionVectorRecordCount,
                       long selectionVectorAddr, long selectionVectorSize,
                       List<ValueVector> outColumns) throws GandivaException {
    if (this.closed) {
      throw new EvaluatorClosedException();
    }

    logger.error("LR Projector.java evaluate");
    if (numExprs != outColumns.size()) {
      logger.info("Expected " + numExprs + " columns, got " + outColumns.size());
      throw new GandivaException("Incorrect number of columns for the output vector");
    }

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    boolean hasVariableWidthColumns = false;
    BaseVariableWidthVector[] resizableVectors = new BaseVariableWidthVector[outColumns.size()];
    
    long[] outAddrs = new long[3 * outColumns.size()];
    long[] outSizes = new long[3 * outColumns.size()];

    idx = 0;
    int outColumnIdx = 0;
    for (ValueVector valueVector : outColumns) {
      if (valueVector instanceof ListVector) {
        //LR HACK there is only one column.
        logger.error("LR Projector.java evaluate out columns=" + outColumns.size());
        outAddrs = new long[5 * outColumns.size()];
        outSizes = new long[5 * outColumns.size()];
      }

      /*boolean isFixedWith = valueVector instanceof FixedWidthVector;*/
      boolean isVarWidth = valueVector instanceof VariableWidthVector;
      /*if (!isFixedWith && !isVarWidth) {
        throw new UnsupportedTypeException(
            "Unsupported value vector type " + valueVector.getField().getFieldType());
      }*/

      outAddrs[idx] = valueVector.getValidityBuffer().memoryAddress();
      outSizes[idx++] = valueVector.getValidityBuffer().capacity();
      if (isVarWidth) {
        outAddrs[idx] = valueVector.getOffsetBuffer().memoryAddress();
        outSizes[idx++] = valueVector.getOffsetBuffer().capacity();
        hasVariableWidthColumns = true;

        // save vector to allow for resizing.
        resizableVectors[outColumnIdx] = (BaseVariableWidthVector) valueVector;
      }
      if (valueVector instanceof StructVector) {
        outAddrs[idx] = ((StructVector) valueVector).getChild("lattitude").getDataBuffer().memoryAddress();
        outSizes[idx++] = ((StructVector) valueVector).getChild("lattitude").getDataBuffer().capacity();
      }
      if (valueVector instanceof ListVector) {
        

        List<ArrowBuf> fieldBufs = ((ListVector) valueVector).getDataVector().getFieldBuffers();
        logger.error("LR Projector.java evaluate ListVector has buffers=" + fieldBufs.size());


        outAddrs[idx] = valueVector.getOffsetBuffer().memoryAddress();
        outSizes[idx++] = valueVector.getOffsetBuffer().capacity();

        //vector valid
        outAddrs[idx] = ((ListVector) valueVector).getDataVector().getValidityBufferAddress();
        outSizes[idx++] = ((ListVector) valueVector).getDataVector().getFieldBuffers().get(0).capacity();
        

        //vector offset
        logger.error("LR Projector.java evaluate ListVector passing data buffer as " + idx);
        outAddrs[idx] = ((ListVector) valueVector).getDataVector().getFieldBuffers().get(0).memoryAddress();
        outSizes[idx++] = ((ListVector) valueVector).getDataVector().getFieldBuffers().get(0).capacity();
        
        //vector data
        //outAddrs[idx] = ((ListVector) valueVector).getDataVector().getFieldBuffers().get(2).memoryAddress();
        //outSizes[idx++] = ((ListVector) valueVector).getDataVector().getFieldBuffers().get(2).capacity();
        
        //LR HACK TODO ((ListVector) valueVector).getDataVector().capacity();










     
      } else {
        outAddrs[idx] = valueVector.getDataBuffer().memoryAddress();
        outSizes[idx++] = valueVector.getDataBuffer().capacity();
      }

      valueVector.setValueCount(selectionVectorRecordCount);
      outColumnIdx++;
    }

    logger.error("LR Projector.java evaluate calling evaluateProjector with buffers=" + idx);
    wrapper.evaluateProjector(
        hasVariableWidthColumns ? new VectorExpander(resizableVectors) : null,
        this.moduleId, numRows, bufAddrs, bufSizes,
        selectionVectorType, selectionVectorRecordCount,
        selectionVectorAddr, selectionVectorSize,
        outAddrs, outSizes);

    //outColumns.clear();
    //FieldType ft = new FieldType(true, int32, null);
    //ListVector lv = new ListVector("res", allocator, ft, null);
    //System.out.println(intVector.getDataVector());


    //logger.error("LR Projector.java after evaluateProjector buffer[3]=" + outAddrs[3][3 * 4]);
    for (ValueVector valueVector : outColumns) {
      if (valueVector instanceof ListVector) {
        //LR HACK

        int numRecordsFound = 5;
        //int numRecordsFound = Math.toIntExact(outSizes[3]) / 4;
        logger.error("LR Projector.java using outsizes numRecords=" + numRecordsFound);

        ArrowBuf ab = new ArrowBuf(ReferenceManager.NO_OP, null, outSizes[2], outAddrs[2]);
        ArrowBuf ab2 = new ArrowBuf(ReferenceManager.NO_OP, null, outSizes[3], outAddrs[3]);
        List<ArrowBuf> outBufsNew = new ArrayList<ArrowBuf>();

        outBufsNew.add(ab);
        outBufsNew.add(ab2);
        ArrowFieldNode afn = new ArrowFieldNode(numRecordsFound, 0);
        ((ListVector) valueVector).getDataVector().clear();
        ((ListVector) valueVector).getDataVector().loadFieldBuffers(afn, outBufsNew);
        //byte[] valid = new byte[outsizes[2]];
        //LR HACK
        //for (int i = 0; i < outSizes[2]; i++) {
        for (int i = 0; i < numRecordsFound * 10; i++) {
          BitVectorHelper.setBit(((ListVector) valueVector).getDataVector().getValidityBuffer(), i);
        }
      }
    }

  }

  /**
   * Closes the LLVM module representing this evaluator.
   */
  public void close() throws GandivaException {
    if (this.closed) {
      return;
    }

    wrapper.closeProjector(this.moduleId);
    this.closed = true;
  }
}
